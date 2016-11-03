#!/usr/bin/env python
"""
Backup DynamoDB to an Avro Container

"""
import argparse
import base64
import json
import logging
import time

import boto3
import fastavro
try:
    import snappy
except ImportError:
    snappy = None

__version__ = '0.1.1'

LOGGER = logging.getLogger(__name__)

CODEC_CHOICES = {'none', 'deflate'}
if snappy:
    CODEC_CHOICES.add('snappy')

LOG_FORMAT = '%(asctime)-15s %(module)s: %(message)s'

records, units = 0, 0


def backup(args):
    """Backup the DynamoDB table

    :param argparse.namespace args: The parsed CLI arguments

    """
    client = boto3.client('dynamodb')
    paginator = client.get_paginator('scan')

    writer = fastavro._writer.Writer(
        args.destination, json.load(args.schema), args.codec,
        validator=None if not args.skip_validation else True)

    verbose = args.verbose or args.debug
    start_time = time.time()

    print('Preparing to backup {} from DynamoDB'.format(args.table))

    try:
        _backup(paginator, args.table, writer)
    except KeyboardInterrupt:
        print('CTRL-C caught, aborting backup')

    print('Wrote {:,} records, consuming {:,} DynamoDB units '
          'in {:.2f} seconds'.format(records, units, time.time() - start_time))


def _backup(paginator, table, writer):
    """Process the backup

    :param boto3.paginator.Paginator paginator: The paginator for getting data
    :param str table: The table name to backup
    :param fastavro._writer.Writer writer: The Avro container writer

    """
    global records, units

    for page in paginator.paginate(TableName=table,
                                   ConsistentRead=True,
                                   ReturnConsumedCapacity='TOTAL'):
        units += page['ConsumedCapacity']['CapacityUnits']
        for record in [_unmarshall(item) for item in page.get('Items', [])]:
            writer.write(record)
            records += 1
        writer.flush()
        LOGGER.debug('%i records written, %i in the last page',
                     records, page['Count'])


def main():
    """Setup and run the backup."""
    args = parse_cli_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
        for logger in {'boto3', 'botocore'}:
            logging.getLogger(logger).setLevel(logging.INFO)
    backup(args)


def parse_cli_args():
    """Construct the CLI argument parser and return the parsed the arguments.

    :rtype: argparse.namespace

    """
    parser = argparse.ArgumentParser(
        'dynamodb-backup',
        description='Backup a DynamoDB table to an Avro Container')
    parser.add_argument('-c', '--codec', choices=CODEC_CHOICES,
                        help='Compression Codec. Default: deflate',
                        default='deflate')
    parser.add_argument('-s', '--skip-validation', action='store_true',
                        help='Do not validate records against the avro schema')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Verbose logging output')
    parser.add_argument('schema', action='store',
                        type=argparse.FileType('r'),
                        help='Avro Schema to use')
    parser.add_argument('table', action='store',
                        help='DynamoDB table name')
    parser.add_argument('destination', action='store',
                        type=argparse.FileType('wb'),
                        help='Destination file path for the backup file')
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
    return dict([(k, _unmarshall_dict(v)) for k, v in values.items()])


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
        return set([base64.b64decode(v.encode('ascii'))
                    for v in value[key]])
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
        return set([_to_number(v) for v in value[key]])
    elif key == 'S':
        return value[key]
    elif key == 'SS':
        return set([v for v in value[key]])
    raise ValueError('Unsupported value type: %s' % key)

if __name__ == '__main__':
    main()
