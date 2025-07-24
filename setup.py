################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import os
import sys

from setuptools import setup, find_packages

this_directory = os.path.abspath(os.path.dirname(__file__))
version_file = os.path.join(this_directory, 'pyfluss/version.py')

try:
    exec(open(version_file).read())
except IOError:
    print("Failed to load PyFluss version file for packaging. " +
          "'%s' not found!" % version_file,
          file=sys.stderr)
    sys.exit(-1)
VERSION = __version__  # noqa

PACKAGES = [
    'pyfluss',
    'pyfluss.api',
    'pyfluss.py4j',
    'pyfluss.py4j.util',
    'pyfluss.py4j.tests',
    'pyfluss.jars'
]

install_requires = [
    'py4j==0.10.9.7',
]

extras_require = {
    'arrow': [
        'pyarrow>=5.0.0'
    ],
    'pandas': [
        'pandas>=1.3.0'
    ],
    'all': [
        'pyarrow>=5.0.0',
        'pandas>=1.3.0',
        'duckdb>=0.5.0,<2.0.0',
        'ray~=2.10.0'
    ]
}

long_description = '''
PyFluss - Python SDK for Apache Fluss

PyFluss provides a Python API for interacting with Apache Fluss, a streaming data lake storage engine.
It offers high-level APIs for reading and writing data, schema management, and query operations.

Features:
- Schema management and introspection
- High-performance data reading and writing
- Integration with pandas and PyArrow
- Streaming and batch processing support
'''

setup(
    name='pyfluss',
    version=VERSION,
    packages=PACKAGES,
    include_package_data=True,
    # JAR files will be included in package
    package_dir={
        "pyfluss.jars": "pyfluss/jars"
    },
    package_data={
        "pyfluss.jars": ["*.jar"]
    },
    install_requires=install_requires,
    extras_require=extras_require,
    entry_points={
        'console_scripts': [
            'pyfluss=pyfluss.cli:main',
        ],
    },
    description='Python SDK for Apache Fluss',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Apache Software Foundation',
    author_email='dev@fluss.apache.org',
    url='https://fluss.apache.org',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12'
    ],
    python_requires='>=3.8'
)
