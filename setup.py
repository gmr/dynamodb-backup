import setuptools


def read_requirements_file(name):
    reqs = []
    try:
        with open(name) as req_file:
            for line in req_file:
                if '#' in line:
                    line = line[0:line.index('#')]
                line = line.strip()
                if line:
                    reqs.append(line)
    except IOError:
        pass
    return reqs


setuptools.setup(
    name='dynamodb-backup',
    version='0.5.0',
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
    install_requires=read_requirements_file('requirements.txt'),
    zip_safe=True,
    entry_points={
        'console_scripts': ['dynamodb-backup=dynamodb_backup:main']
    },
    extras_require={'snappy': ['python-snappy==0.5']})
