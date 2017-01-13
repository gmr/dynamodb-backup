#!/usr/bin/env python
"""
Backup DynamoDB to an Avro Container

"""
import argparse
import base64
import coloredlogs
import datetime
import io
import json
import logging
import sys
import time

import boto3
import fastavro
import requests
try:
    import snappy
except ImportError:
    snappy = None

__version__ = '0.3.0'

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
records, units = 0, 0


def backup(args):
    """Backup the DynamoDB table

    :param argparse.namespace args: The parsed CLI arguments

    """
    client = boto3.client('dynamodb')
    paginator = client.get_paginator('scan')

    start_time = time.time()

    if args.schema.startswith('http'):
        response = requests.get(args.schema)
        schema = response.json()
    else:
        with open(args.schema, 'r') as handle:
            schema = json.load(handle)

    if args.destination:
        handle = open(args.destination, 'wb')
        dest = args.destination
    elif args.bucket:
        handle = io.BytesIO()
        dest = 's3://{}/{}'.format(args.bucket, args.prefix)
    else:
        LOGGER.error('ERROR: Must specify an error or an S3 bucket to use')
        sys.exit(1)

    LOGGER.info('Preparing to backup %s from DynamoDB to %s',
                args.table, dest)

    try:
        fastavro.writer(
            handle, schema,
            _backup(paginator, args.table),
            args.codec,
            validator=None if not args.skip_validation else True)

    except KeyboardInterrupt:
        LOGGER.error('CTRL-C caught, aborting backup')
        sys.exit(1)

    LOGGER.info(
        'Wrote {:,} records, consuming {:,} DynamoDB units in '
        '{:.2f} seconds'.format(records, units, time.time() - start_time))

    if args.destination:
        handle.close()
    elif args.bucket:
        _upload_to_s3(args, handle)


def _backup(paginator, table):
    """Generator to return the records from the DynamoDB table

    :param boto3.paginator.Paginator paginator: The paginator for getting data
    :param str table: The table name to backup

    """
    global records, units

    for page in paginator.paginate(TableName=table,
                                   ConsistentRead=True,
                                   ReturnConsumedCapacity='TOTAL'):
        units += page['ConsumedCapacity']['CapacityUnits']
        for record in [_unmarshall(item) for item in page.get('Items', [])]:
            yield record
            records += 1
        LOGGER.debug('%i records written, %i in the last page',
                     records, page['Count'])


def main():
    """Setup and run the backup."""
    args = parse_cli_args()
    level = logging.DEBUG if args.verbose else logging.INFO
    coloredlogs.install(level=level, fmt=LOGGING_FORMAT,
                        field_styles=LOGGING_FIELD_STYLES)
    if args.verbose:
        for logger in {
                'boto3', 'botocore', 'requests',
                'botocore.vendored.requests.packages.urllib3.connectionpool'}:
            logging.getLogger(logger).setLevel(logging.WARNING)
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

    parser.add_argument('-b', '--bucket', action='store',
                        help='S3 Bucket to upload the file to')

    parser.add_argument('-d', '--destination', action='store',
                        help='Local path to write the file to.')

    parser.add_argument('-p', '--prefix', action='store', default='dynamodb',
                        help='The S3 Bucket base path to use. '
                             'Default: dynamodb/')

    parser.add_argument('-s', '--skip-validation', action='store_true',
                        help='Do not validate records against the avro schema')

    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Verbose logging output')

    parser.add_argument('schema', action='store',
                        help='Avro Schema to use')
    parser.add_argument('table', action='store',
                        help='DynamoDB table name')
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


def _upload_to_s3(args, handle):
    """Create the backup as a file on AmazonS3

    :param argparse.namespace args: The parsed CLI arguments
    :param io.BytesIO handle: The BytesIO handle for the backup

    """
    handle.seek(0)
    backup_path = datetime.date.today().strftime(
        '{}/%Y/%m/%d/{}.avro'.format(args.prefix, args.table))
    s3 = boto3.resource('s3')
    s3.Object(args.bucket, backup_path).put(Body=handle)


if __name__ == '__main__':
    main()
