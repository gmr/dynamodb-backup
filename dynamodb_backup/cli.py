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
import pathlib
import queue
import sys
import tempfile
import time
import typing
from urllib import parse

import boto3
from botocore import exceptions
import coloredlogs
import fastavro
from fastavro import validation
import requests
try:
    import snappy
except ImportError:
    snappy = None

from dynamodb_backup import version

LOGGER = logging.getLogger(__name__)
LOGGERS = {
    'boto3',
    'botocore',
    'botocore.vendored.requests.packages.urllib3.connectionpool',
    'requests',
    's3transfer',
    'urllib3'}

CODEC_CHOICES = {'none', 'deflate'}
if snappy:
    CODEC_CHOICES.add('snappy')

LOGGING_FORMAT = '%(asctime)s %(levelname) -10s %(name) -10s %(message)s'
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
    def __init__(self, group=None, target=None, name=None,
                 args=None, kwargs=None):
        super().__init__(group, target, name, args or (), kwargs or {})
        self.cli_args = kwargs['cli_args']
        self.error_exit = kwargs['error_exit']
        self.finished = kwargs['finished']
        self.queued = kwargs['queued']
        self.processed = kwargs['processed']
        self.segment = kwargs['segment']
        self.units = kwargs['unit_count']
        self.work_queue = kwargs['work_queue']

    def run(self) -> typing.NoReturn:
        """Executes on start, paginate through the table"""
        _configure_logging(self.cli_args)
        client = boto3.client('dynamodb')
        paginator = client.get_paginator('scan')
        LOGGER.debug('Starting to page through %s for segment %s of %s',
                     self.cli_args.table, self.segment + 1,
                     self.cli_args.processes)
        kwargs = {'TableName': self.cli_args.table,
                  'TotalSegments': self.cli_args.processes,
                  'ConsistentRead': True,
                  'PaginationConfig': {'PageSize': 500},
                  'ReturnConsumedCapacity': 'TOTAL',
                  'Segment': self.segment}

        if self.cli_args.expression:
            self.cli_args.expression.seek(0)
            kwargs.update(json.load(self.cli_args.expression))

        queued = 0
        for page in paginator.paginate(**kwargs):
            if self.error_exit.is_set():
                break
            for row in [_unmarshall(i) for i in page.get('Items', [])]:
                self.work_queue.put(row)
                with self.queued.get_lock():
                    self.queued.value += 1
                queued += 1
                if self.error_exit.is_set():
                    break
            with self.units.get_lock():
                self.units.value += page['ConsumedCapacity']['CapacityUnits']
            while self._queue_size(queued) > 0:
                if self.error_exit.is_set():
                    break
                LOGGER.debug('Waiting for %i items in queue',
                             self._queue_size(queued))
                try:
                    time.sleep(0.25)
                except KeyboardInterrupt:
                    self.error_exit.set()
                    break

        LOGGER.debug('Finished paginating in segment %i', self.segment + 1)
        with self.finished.get_lock():
            self.finished.value += 1
        if self.finished.value == self.cli_args.processes:
            LOGGER.debug('Closing the queue')
            self.work_queue.close()
        LOGGER.debug('Exiting process %i', self.segment + 1)

    def _queue_size(self, queued: int) -> int:
        try:
            return self.work_queue.qsize()
        except NotImplementedError:
            return queued - self.processed.value


class QueueIterator:
    """Generator to return the records from the DynamoDB table"""
    def __init__(self,
                 error_exit: multiprocessing.Event,
                 finished: multiprocessing.Value,
                 processed: multiprocessing.Value,
                 queued: multiprocessing.Value,
                 segments: int,
                 status_interval: int,
                 units: multiprocessing.Value,
                 work_queue: multiprocessing.Queue):
        self.error_exit = error_exit
        self.finished = finished
        self.processed = processed
        self.queued = queued
        self.records = 0
        self.segments = segments
        self.status_interval = status_interval or 10000
        self.units = units
        self.work_queue = work_queue

    def __iter__(self):
        return self

    def __next__(self) -> dict:
        if self._should_exit:
            LOGGER.debug('%i items in queue, %i processed',
                         self._queue_size, self.processed.value)
            LOGGER.debug('Exiting iterator on __next__')
            raise StopIteration
        while not self._should_exit:
            try:
                return self._get_record()
            except queue.Empty:
                LOGGER.debug('Queue get timeout %r', self._should_exit)
                if self._should_exit:
                    LOGGER.debug('Exiting iterator on __next__ queue.Empty')
                    raise StopIteration
        LOGGER.debug('%i items in queue, %i processed',
                     self._queue_size, self.processed.value)
        raise StopIteration

    def _get_record(self) -> dict:
        record = self.work_queue.get(True, 3)
        self.records += 1
        if self.records % self.status_interval == 0:
            LOGGER.debug(
                'Wrote %i records, %.2f units consumed, %i records queued',
                self.records, self.units.value, self._queue_size)
        return record

    @property
    def _queue_size(self) -> int:
        try:
            return self.work_queue.qsize()
        except NotImplementedError:
            return self.records - self.processed.value

    @property
    def _should_exit(self) -> bool:
        return (self.error_exit.is_set() or
                (self.finished.value == self.segments and
                 self.queued.value == self.processed.value))


class Writer:

    EXTENSION = 'txt'

    def __init__(self,
                 args: argparse.Namespace,
                 iterator: QueueIterator,
                 error_exit: multiprocessing.Event,
                 processed: multiprocessing.Value):
        self.args = args
        self.chunk = 0
        self.dirty = False
        self.error_exit = error_exit
        self.iterator = iterator
        self.processed = processed
        self.s3uri = self.args.s3.rstrip('/') if self.args.s3 else None
        self.file = self._get_file()

    def close(self) -> typing.NoReturn:
        if self.s3uri and not self.error_exit.is_set() and self.dirty:
            LOGGER.debug('Uploading to S3 on close')
            self._upload_to_s3()
        if not self.file.closed:
            self.file.close()
        if not self.dirty:
            LOGGER.debug('Removing empty final file')
            self._file_path.unlink()

    @property
    def destination(self) -> pathlib.Path:
        return self.s3uri if self.s3uri else pathlib.Path(self.args.directory)

    @property
    def extension(self) -> str:
        return self.EXTENSION

    @property
    def _file_path(self) -> pathlib.Path:
        if self.args.chunk:
            return self.destination / '{}-{:03d}.{}'.format(
                self.args.table, self.chunk, self.extension)
        return self.destination / '{}.{}'.format(
            self.args.table, self.extension)

    def _get_file(self) -> typing.Union[tempfile.NamedTemporaryFile,
                                        typing.BinaryIO]:
        self.dirty = False
        if self.args.s3:
            return tempfile.NamedTemporaryFile('xb+')
        return self._file_path.open('wb+')

    def _s3_key(self):
        parsed = parse.urlparse(self.s3uri, 's3')
        filename = '{}.{}'.format(self.args.table, self.extension)
        if self.args.chunk:
            filename = '{}-{:03d}.{}'.format(
                self.args.table, self.chunk, self.extension)
        return datetime.date.today().strftime(
                '{}/%Y/%m/%d/{}'.format(parsed.path.strip('/'), filename))

    def _upload_to_s3(self):
        """Create the backup as a file on AmazonS3"""
        self.file.flush()
        self.file.seek(0)
        parsed = parse.urlparse(self.s3uri, 's3')
        key = self._s3_key()
        LOGGER.info('Uploading to s3://%s/%s', parsed.netloc, key)
        try:
            boto3.client('s3').upload_file(self.file.name, parsed.netloc, key)
        except exceptions.ClientError as error:
            LOGGER.error('Error uploading: %s', error)
            self.error_exit.set()


class AvroWriter(Writer):

    EXTENSION = 'avro'

    def __init__(self,
                 args: argparse.Namespace,
                 iterator: QueueIterator,
                 error_exit: multiprocessing.Event,
                 processed: multiprocessing.Value,
                 schema: dict):
        super().__init__(args, iterator, error_exit, processed)
        self.schema = schema

    def write_from_queue(self) -> typing.NoReturn:
        if self.args.chunk:
            rows = []
            for row in self._iterator:
                rows.append(row)
                if len(rows) == self.args.chunk_size:
                    fastavro.writer(
                        self.file, self.schema, rows, self.args.codec)
                    if self.s3uri:
                        self._upload_to_s3()
                    self.file.close()
                    self.chunk += 1
                    self.file = self._get_file()
                    rows = []
            # Write the remaining rows
            if rows:
                self.dirty = True
                fastavro.writer(
                    self.file, self.schema, rows, self.args.codec)
        else:
            self.dirty = True
            fastavro.writer(
                self.file, self.schema, self._iterator, self.args.codec)
        LOGGER.debug('Exiting writer')

    @property
    def _iterator(self):
        for record in self.iterator:
            self.processed.value += 1
            if self._validate(record):
                yield record

    def _validate(self, record: dict) -> bool:
        try:
            validation.validate(record, self.schema)
        except validation.ValidationError as error:
            LOGGER.warning('Record failed to validate: %s',
                           str(error).replace('\n', ''))
            LOGGER.debug('Invalid record: %r', record)
            return False
        return True


class CSVWriter(Writer):

    EXTENSION = 'csv'

    def __init__(self,
                 args: argparse.Namespace,
                 iterator: QueueIterator,
                 error_exit: multiprocessing.Event,
                 processed: multiprocessing.Value):
        super().__init__(args, iterator, error_exit, processed)
        self.fields = self._get_fields()

    def close(self) -> typing.NoReturn:
        super(CSVWriter, self).close()
        if not self.dirty and not self.s3uri:
            self._file_path.unlink()

    @property
    def extension(self) -> str:
        if self.args.compress:
            return '{}.gz'.format(self.EXTENSION)
        return self.EXTENSION

    def write_from_queue(self) -> typing.NoReturn:
        handle, writer = self._get_writer()
        for record in self.iterator:
            self.dirty = True
            writer.writerow(record)
            self.processed.value += 1
            if self.args.chunk and \
                    self.processed.value % self.args.chunk_size == 0:
                self.chunk += 1
                self._finish_file(handle)
                handle, writer = self._get_writer()
        self._finish_file(handle, False)

    def _finish_file(self, handle: typing.TextIO,
                     open_new: bool = True) -> typing.NoReturn:
        handle.seek(0)
        if self.args.compress:
            self.file.write(gzip.compress(handle.read().encode('UTF-8')))
        else:
            self.file.write(handle.read().encode('UTF-8'))
        if self.s3uri:
            self._upload_to_s3()
        self.file.close()
        if open_new:
            self.file = self._get_file()

    def _get_fields(self) -> list:
        client = boto3.client('dynamodb')
        result = client.scan(TableName=self.args.table, Limit=1,
                             Select='ALL_ATTRIBUTES')
        return sorted(result['Items'][0].keys())

    def _get_writer(self) -> (typing.TextIO, csv.DictWriter):
        handle = io.StringIO()
        writer = csv.DictWriter(handle, self.fields)
        if self.args.write_header:
            writer.writeheader()
        return handle, writer


def main() -> typing.NoReturn:
    """Setup and run the backup."""
    args = _parse_cli_args()
    _configure_logging(args)
    _execute(args)


def _configure_logging(args: argparse.Namespace) -> typing.NoReturn:
    level = logging.DEBUG if args.verbose else logging.INFO
    coloredlogs.install(
        level=level, fmt=LOGGING_FORMAT, field_styles=LOGGING_FIELD_STYLES)
    for logger in LOGGERS:
        logging.getLogger(logger).setLevel(logging.WARNING)


def _execute(args: argparse.Namespace) -> typing.NoReturn:
    """What executes when your application is run. It will spawn the
    specified number of threads, reading in the data from the specified file,
    adding them to the shared queue. The worker threads will work the queue,
    and the application will exit when the queue is empty.

    """
    error_exit = multiprocessing.Event()
    finished = multiprocessing.Value('f', 0, lock=True)
    processed = multiprocessing.Value('f', 0, lock=True)
    queued = multiprocessing.Value('f', 0, lock=True)
    units = multiprocessing.Value('f', 0, lock=True)
    work_queue = multiprocessing.Queue()

    iterator = QueueIterator(
        error_exit, finished, processed, queued, args.processes,
        args.chunk_size, units, work_queue)

    if args.format == 'csv':
        writer = CSVWriter(args, iterator, error_exit, processed)
    else:
        writer = AvroWriter(
            args, iterator, error_exit, processed, _schema(args.schema))

    processes = []
    LOGGER.debug('Creating %i processes', args.processes)
    for index in range(0, args.processes):
        proc = Process(kwargs={'cli_args': args,
                               'error_exit': error_exit,
                               'finished': finished,
                               'processed': processed,
                               'queued': queued,
                               'segment': index,
                               'unit_count': units,
                               'work_queue': work_queue})
        proc.daemon = True
        proc.start()
        processes.append(proc)

    LOGGER.info('Preparing to backup %s from DynamoDB to %s',
                args.table, writer.destination)

    start_time = time.time()
    try:
        writer.write_from_queue()
    except KeyboardInterrupt:
        LOGGER.error('CTRL-C caught, aborting backup')
        error_exit.set()

    if error_exit.is_set():
        LOGGER.error('Exiting in error')
        sys.exit(1)

    for proc in processes:
        if proc.is_alive():
            proc.terminate()

    # Wait for processes to close out
    while sum(1 if p.is_alive() else 0 for p in processes):
        alive = sum(1 if p.is_alive() else 0 for p in processes)
        LOGGER.debug('Waiting for %i processes to finish', alive)
        try:
            time.sleep(0.5)
        except KeyboardInterrupt:
            error_exit.set()
            break

    writer.close()
    LOGGER.info(
        'Queued %i records and wrote %i records, using %i DynamoDB '
        'units in %.2f seconds',
        queued.value, writer.iterator.records, units.value,
        time.time() - start_time)


def _parse_cli_args() -> argparse.Namespace:
    """Construct the CLI argument parser and return the parsed the arguments"""
    parser = argparse.ArgumentParser(
        description='Backup a DynamoDB table',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    dest = parser.add_mutually_exclusive_group(required=True)
    dest.add_argument('-d', '--directory',
                      help='Write the output to the specified directory')
    dest.add_argument('--s3', help='S3 URI to upload output to')

    f_parser = parser.add_subparsers(title='Output Format', dest='format')
    csv_parser = f_parser.add_parser('csv', help='Output to CSV')
    csv_parser.add_argument('-c', '--compress', action='store_true',
                            help='GZip compress CSV output')
    csv_parser.add_argument('-w', '--write-header', action='store_true',
                            help='Write the CSV header line')
    avro_parser = f_parser.add_parser('avro', help='Output to Avro Container')
    avro_parser.add_argument('-c', '--codec', choices=CODEC_CHOICES,
                             default='deflate', help='Compression Codec')
    avro_parser.add_argument('-s', '--schema', required=True,
                             help='Avro Schema to use')

    f_parser = parser.add_argument_group('Filter Expression Options')
    f_parser.add_argument('-e', '--expression', type=argparse.FileType('r'),
                          help='A JSON file with filter expression '
                               'kwargs for the paginator')
    parser.add_argument('-k', '--chunk', action='store_true',
                        help='Write chunks to multiple-files')
    parser.add_argument('-l', '--chunk-size', action='store', type=int,
                        default=100000,
                        help='Record count limit when writing in chunks')
    parser.add_argument('-n', '--processes', default=1, type=int,
                        metavar='COUNT',
                        help='Total number of processes for scan.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Verbose logging output')
    parser.add_argument('--version', action='version',
                        version='%(prog)s {}'.format(version))

    parser.add_argument('table', action='store', help='DynamoDB table name')
    return parser.parse_args()


def _schema(file_path: str) -> dict:
    if file_path.startswith('http'):
        response = requests.get(file_path)
        return response.json()
    with open(file_path, 'r') as handle:
        return json.load(handle)


def _to_number(value: str) -> typing.Union[float, int]:
    """Convert the string containing a number to a number"""
    return float(value) if '.' in value else int(value)


def _unmarshall(values: dict) -> dict:
    """Transform a response payload from DynamoDB to a native dict

    :raises ValueError: if an unsupported type code is encountered

    """
    return {k: _unmarshall_dict(v) for k, v in values.items()}


def _unmarshall_dict(value: dict) -> typing.Any:
    """Unmarshall a single dict value from a row that was returned from
    DynamoDB, returning the value as a normal Python dict.

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
