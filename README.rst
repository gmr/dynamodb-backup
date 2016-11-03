dynamodb-backup
===============
Backup a `DynamoDB <https://aws.amazon.com/dynamodb/>`_ table to an
`Avro <http://avro.apache.org>`_ container.

Currently, only full table backups are supported.

|Version|

Installation
------------
``dynamodb-backup`` is available on the
`Python Package Index <https://pypi.python.org>`_:

.. code:: bash

    pip install dynamodb-backup

dynamodb-backup optionally supports the `snappy <https://google.github.io/snappy/>`_
codec for compression if the Python snappy package is installed. This can be
installed using the pip extras install:

.. code:: bash

    pip install dynamodb-backup[snappy]

Usage
-----
::

   dynamodb-backup --help
   usage: dynamodb-backup [-h] [-c {snappy,none,deflate}] [-s] [-v]
                          schema table destination

   Backup a DynamoDB table to an Avro Container

   positional arguments:
     schema                Avro Schema to use
     table                 DynamoDB table name
     destination           Destination file path for the backup file

   optional arguments:
     -h, --help            show this help message and exit
     -c {snappy,none,deflate}, --codec {snappy,none,deflate}
                           Compression Codec. Default: deflate
     -s, --skip-validation
                           Do not validate records against the avro schema
     -v, --verbose         Verbose logging output

.. |Version| image:: https://img.shields.io/pypi/v/dynamodb-backup.svg?
   :target: https://pypi.python.org/pypi/dynamodb-backup
