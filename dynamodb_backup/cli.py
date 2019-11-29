#!/usr/bin/env python3
"""
Backup DynamoDB to CSV or Avro

"""
import argparse
import base64
import csv
import datetime
import gzip
import io
import json
import logging
import multiprocessing
import queue
import sys
import tempfile
import time
from urllib import parse

import boto3
from botocore import exceptions
import coloredlogs
import fastavro
import httpx
try:
    import snappy
except ImportError:
    snappy = None

from dynamodb_backup import version


LOGGER = logging.getLogger(__name__)

CODEC_CHOICES = {'none', 'deflate'}
if snappy:
    CODEC_CHOICES.add('snappy')

LOGGING_FORMAT = '%(asctime)s %(message)s'
LOGGING_FIELD_STYLES = {'hostname': {'color': 'magenta'},
                        'programname': {'color': 'cyan'},
                        'name': {'color': 'blue'},
                        'levelname': {'color': 'white', 'bold': True},
                        'asctime': {'color': 'white'}}

MAX_QUEUE_DEPTH = 100000


class Process(multiprocessing.Process):
    """Child process for paginating from DynamoDB and putting rows in the work
    queue.

    """
    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}):
        super(Process, self).__init__(group, target, name, args, kwargs)
        self.cliargs = kwargs['cliargs']
        self.done = kwargs['done']
        self.error_exit = kwargs['error_exit']
        self.segment = kwargs['segment']
        self.units = kwargs['unit_count']
        self.work_queue = kwargs['work_queue']

    @property
    def queue_size(self):
        try:
            return self.work_queue.qsize()
        except NotImplementedError:
            return -1

    def run(self):
        """Executes on start, paginate through the table"""
        client = boto3.client('dynamodb')
        paginator = client.get_paginator('scan')
        LOGGER.info('Starting to page through %s for segment %s of %s',
                    self.cliargs.table, self.segment, self.cliargs.processes)

        kwargs = {'TableName': self.cliargs.table,
                  'TotalSegments': self.cliargs.processes,
                  'ConsistentRead': True,
                  'PaginationConfig': {'PageSize': 500},
                  'ReturnConsumedCapacity': 'TOTAL',
                  'Segment': self.segment}

        if self.cliargs.expression:
            self.cliargs.expression.seek(0)
            kwargs.update(json.load(self.cliargs.expression))

        for page in paginator.paginate(**kwargs):
            for record in [_unmarshall(item) for item in
                           page.get('Items', [])]:
                self.work_queue.put(record)
                if self.error_exit.is_set():
                    break
            with self.units.get_lock():
                self.units.value += page['ConsumedCapacity']['CapacityUnits']
            while self.queue_size:
                try:
                    time.sleep(0.1)
                except KeyboardInterrupt:
                    self.error_exit.set()
                    break
            if self.error_exit.is_set():
                break
        self.done.set()


class QueueIterator:

    def __init__(self, error_exit, work_queue, units):
        """Generator to return the records from the DynamoDB table

        :param multiprocessing.Event error_exit: Set on error to stop
        :param multiprocessing.Queue work_queue: The queue to get records from
        :param multiprocessing.Value units: Keep track of consumed units

        """
        self.records = 0
        self.error_exit = error_exit
        self.work_queue = work_queue
        self.units = units

    def __iter__(self):
        return self

    def __next__(self):
        if self.error_exit.is_set():
            raise StopIteration
        try:
            record = self.work_queue.get(True)
        except queue.Empty:
            raise StopIteration
        self.records += 1
        if self.records % 10000 == 0:
            try:
                queue_size = self.work_queue.qsize()
            except NotImplementedError:
                queue_size = -1
            LOGGER.debug(
                '%i records written, %.2f units consumed, %i records queued',
                self.records, self.units.value, queue_size)
        return record


class Writer:

    EXTENSION = 'txt'

    def __init__(self, args, iterator, error_exit):
        self.args = args
        self.error_exit = error_exit
        self.file = None
        self.handle = self._get_handle()
        self.iterator = iterator
        self.s3uri = self.args.s3.rstrip('/') if self.args.s3 else None

    def close(self):
        if self.s3uri and not self.error_exit.is_set():
            self._upload_to_s3()
        self.handle.close()

    @property
    def destination(self):
        if self.s3uri:
            parsed = parse.urlparse(self.s3uri, 's3')
            key = datetime.date.today().strftime(
                '/%Y/%m/%d/{}.{}'.format(self.args.table, self.extension))
            return 's3://{}{}'.format(parsed.netloc, key)
        return self.file

    @property
    def extension(self):
        return self.EXTENSION

    def _get_handle(self):
        if self.args.s3:
            return tempfile.NamedTemporaryFile()
        return open(self.args.file, 'wb')

    def _upload_to_s3(self):
        """Create the backup as a file on AmazonS3"""
        parsed = parse.urlparse(self.s3uri, 's3')
        key = datetime.date.today().strftime(
            '/%Y/%m/%d/{}.{}'.format(self.args.table, self.extension))
        LOGGER.info('Uploading to s3://%s%s', parsed.netloc, key)
        s3 = boto3.client('s3')
        try:
            s3.upload_file(self.handle.name, parsed.netloc, key)
        except exceptions.ClientError as error:
            LOGGER.error('Error uploading: %s', error)


class AvroWriter(Writer):

    EXTENSION = 'avro'

    def __init__(self, args, iterator, error_exit):
        super(AvroWriter, self).__init__(args, iterator, error_exit)
        if args.schema.startswith('http'):
            response = httpx.get(args.schema)
            self.schema = response.json()
        else:
            with open(args.schema, 'r') as handle:
                self.schema = json.load(handle)

    def write_from_queue(self):
        fastavro.writer(
            self.handle, self.schema, self.iterator, self.args.codec,
            validator=None if not self.args.skip_validation else True)


class CSVWriter(Writer):

    EXTENSION = 'csv'

    def __init__(self, args, iterator, error_exit):
        self.file = None
        super(CSVWriter, self).__init__(args, iterator, error_exit)
        self.writer = csv.DictWriter(self.handle, self._get_fields())
        if args.write_header:
            self.writer.writeheader()

    def close(self):
        super(CSVWriter, self).close()
        if self.file:
            self.file.close()

    @property
    def extension(self):
        if self.args.compress:
            return '{}.gz'.format(self.EXTENSION)
        return self.EXTENSION

    def write_from_queue(self):
        for record in self.iterator:
            self.writer.writerow(record)

    def _get_fields(self):
        client = boto3.client('dynamodb')
        result = client.scan(TableName=self.args.table, Limit=1,
                             Select='ALL_ATTRIBUTES')
        keys = sorted(result['Items'][0].keys())
        LOGGER.debug('Keys: %r', keys)
        return sorted(keys)

    def _get_handle(self):
        if self.args.s3:
            if self.args.compress:
                self.file = tempfile.NamedTemporaryFile(mode='wb')
                return io.TextIOWrapper(gzip.GzipFile(fileobj=self.file))
            return tempfile.NamedTemporaryFile()
        elif self.args.compress:
            return gzip.open(self.args.file, 'wt')
        return open(self.args.file, 'w')


def main():
    """Setup and run the backup."""
    args = _parse_cli_args()
    level = logging.DEBUG if args.verbose else logging.INFO
    coloredlogs.install(
        level=level, fmt=LOGGING_FORMAT, field_styles=LOGGING_FIELD_STYLES)
    if args.verbose:
        for logger in {'boto3', 'botocore', 'httpx'}:
            logging.getLogger(logger).setLevel(logging.WARNING)
    _execute(args)


def _execute(args):
    """What executes when your application is run. It will spawn the
    specified number of threads, reading in the data from the specified file,
    adding them to the shared queue. The worker threads will work the queue,
    and the application will exit when the queue is empty.

    :param argparse.namespace args: The parsed cli arguments

    """
    error_exit = multiprocessing.Event()
    work_queue = multiprocessing.Queue()
    units = multiprocessing.Value('f', 0, lock=True)

    iterator = QueueIterator(error_exit, work_queue, units)

    if args.format == 'csv':
        writer = CSVWriter(args, iterator, error_exit)
    else:
        writer = AvroWriter(args, iterator, error_exit)

    processes = []
    LOGGER.debug('Creating %i processes', args.processes)
    for index in range(0, args.processes):
        done = multiprocessing.Event()
        proc = Process(kwargs={'done': done,
                               'error_exit': error_exit,
                               'segment': index,
                               'cliargs': args,
                               'unit_count': units,
                               'work_queue': work_queue})
        proc.start()
        processes.append((proc, done))

    LOGGER.info('Preparing to backup %s from DynamoDB to %s',
                args.table, writer.destination)

    start_time = time.time()
    try:
        writer.write_from_queue()
    except KeyboardInterrupt:
        LOGGER.error('CTRL-C caught, aborting backup')
        error_exit.set()

    # All of the data has been added to the queue, wait for things to finish
    LOGGER.debug('Waiting for processes to finish')
    while any(p.is_alive() for (p, d) in processes):
        try:
            time.sleep(0.1)
        except KeyboardInterrupt:
            error_exit.set()
            break

    writer.close()
    LOGGER.info('Wrote {:,} records, consuming {:,} DynamoDB units in '
                '{:.2f} seconds'.format(
                    writer.iterator.records, units.value,
                    time.time() - start_time))
    sys.exit(0)


def _parse_cli_args():
    """Construct the CLI argument parser and return the parsed the arguments.

    :rtype: argparse.namespace

    """
    parser = argparse.ArgumentParser(
        description='Backup a DynamoDB table',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    format = parser.add_subparsers(title='Output Format', dest='format')

    csv_parser = format.add_parser('csv', help='Output to CSV')
    csv_parser.add_argument('-c', '--compress', action='store_true',
                            help='GZip compress CSV output')
    csv_parser.add_argument('-w', '--write-header', action='store_true',
                            help='Write the CSV header line')

    avro_parser = format.add_parser('avro', help='Output to Avro Container')

    avro_parser.add_argument('-c', '--codec', choices=CODEC_CHOICES,
                             default='deflate', help='Compression Codec')
    avro_parser.add_argument('-S', '--skip-validation', action='store_true',
                             help='Do not validate records')
    avro_parser.add_argument('-s', '--schema', required=True,
                             help='Avro Schema to use')

    dest = parser.add_mutually_exclusive_group(required=True)
    dest.add_argument('-f', '--file',
                      help='Write the output to the specified file')
    dest.add_argument('--s3', help='S3 URI to upload output to')

    parser.add_argument('-n', '--processes', default=1, type=int,
                        metavar='COUNT',
                        help='Total number of processes for scan.')

    filter = parser.add_argument_group('Filter Expression Options')
    filter.add_argument('-e', '--expression', type=argparse.FileType('r'),
                        help='A JSON file with filter expression '
                             'kwargs for the paginator')

    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Verbose logging output')
    parser.add_argument('--version', action='version',
                        version='%(prog)s {}'.format(version))

    parser.add_argument('table', action='store', help='DynamoDB table name')
    return parser.parse_args()


def _to_number(value):
    """Convert the string containing a number to a number

    :param str value: The value to convert
    :rtype: float|int

    """
    return float(value) if '.' in value else int(value)


def _unmarshall(values):
    """Transform a response payload from DynamoDB to a native dict

    :param dict values: The response payload from DynamoDB
    :rtype: dict
    :raises ValueError: if an unsupported type code is encountered

    """
    return {k: _unmarshall_dict(v) for k, v in values.items()}


def _unmarshall_dict(value):
    """Unmarshall a single dict value from a row that was returned from
    DynamoDB, returning the value as a normal Python dict.

    :param dict value: The value to unmarshall
    :rtype: mixed
    :raises ValueError: if an unsupported type code is encountered

    """
    key = list(value.keys()).pop()
    if key == 'B':
        return base64.b64decode(value[key].encode('ascii'))
    elif key == 'BS':
        return {base64.b64decode(v.encode('ascii')) for v in value[key]}
    elif key == 'BOOL':
        return value[key]
    elif key == 'L':
        return [_unmarshall_dict(v) for v in value[key]]
    elif key == 'M':
        return _unmarshall(value[key])
    elif key == 'NULL':
        return None
    elif key == 'N':
        return _to_number(value[key])
    elif key == 'NS':
        return {_to_number(v) for v in value[key]}
    elif key == 'S':
        return value[key]
    elif key == 'SS':
        return set(value[key])
    raise ValueError('Unsupported value type: %s' % key)
