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

# High-level API (recommended for users)
from .connection import connect, disconnect, get_connection, FlussConnection

# Alias for convenience
Connection = FlussConnection

from .writer import FlussDataWriter
from .reader import FlussDataReader

# Version information
from .version import __version__

# Schema and configuration API
from .api import Schema

# Low-level API (for advanced users)
from .py4j import Catalog, Table, ReadBuilder, TableScan, Plan, RowType
from .py4j import FlussTableRead, BatchWriteBuilder, BatchTableWrite, FlussSchema

# Main exports - high-level API first
__all__ = [
    # High-level API (primary interface)
    'connect',
    'disconnect', 
    'get_connection',
    'FlussConnection',
    'Connection',  # Alias for FlussConnection
    'FlussDataWriter',
    'FlussDataReader',
    
    # Version and schema
    '__version__',
    'Schema',
    
    # Low-level API (for advanced usage)
    'Catalog',
    'Table', 
    'ReadBuilder',
    'TableScan',
    'Plan',
    'RowType',
    'FlussTableRead',
    'BatchWriteBuilder',
    'BatchTableWrite',
    'FlussSchema'
]
