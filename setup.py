try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(name='dynamodb-backup',
      version='0.1.0',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Environment :: Console',
          'Environment :: No Input/Output (Daemon)',
          'Intended Audience :: System Administrators',
          'License :: OSI Approved :: BSD License',
          'Natural Language :: English',
          'Operating System :: MacOS',
          'Operating System :: POSIX',
          'Operating System :: POSIX :: BSD',
          'Operating System :: POSIX :: Linux',
          'Operating System :: Unix',
          'Programming Language :: Python :: 2',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: Implementation :: CPython',
          'Programming Language :: Python :: Implementation :: PyPy'],
      description='Backup a DynamoDB table to an Avro container',
      long_description=open('README.rst').read(),
      license='BSD',
      author='Gavin M. Roy',
      author_email='gavinmroy@gmail.com',
      py_modules=['dynamodb_backup'],
      package_data={'': ['LICENSE', 'README.rst']},
      install_requires=['boto3', 'fastavro'],
      zip_safe=True,
      entry_points={
          'console_scripts': ['dynamodb-backup=dynamodb_backup:main']
      },
      extras_require={'snappy': ['snappy']})
