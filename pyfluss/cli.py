#!/usr/bin/env python3
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

"""
Pyfluss command-line interface.

This module provides a command-line interface for common Pyfluss operations.
"""

import argparse
import sys
import os
from typing import Optional


def cmd_version() -> None:
    """Print version information."""
    try:
        import pyfluss
        print(f"Pyfluss version: {pyfluss.__version__}")
        print(f"Python version: {sys.version}")
    except ImportError as e:
        print(f"Error importing pyfluss: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_info() -> None:
    """Print package information."""
    try:
        import pyfluss
        package_dir = os.path.dirname(pyfluss.__file__)
        jar_dir = os.path.join(package_dir, 'jars')
        
        print(f"Pyfluss version: {pyfluss.__version__}")
        print(f"Package location: {package_dir}")
        print(f"JAR directory: {jar_dir}")
        
        if os.path.exists(jar_dir):
            jar_files = [f for f in os.listdir(jar_dir) if f.endswith('.jar')]
            print(f"JAR files: {jar_files}")
        else:
            print("JAR directory not found!")
            
        # Check available classes
        available_classes = [attr for attr in dir(pyfluss) 
                           if not attr.startswith('_')]
        print(f"Available classes: {available_classes}")
        
    except ImportError as e:
        print(f"Error importing pyfluss: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_validate() -> None:
    """Validate package installation."""
    try:
        import pyfluss
        print("✓ Pyfluss package imported successfully")
        
        # Test version
        if hasattr(pyfluss, '__version__'):
            print(f"✓ Version accessible: {pyfluss.__version__}")
        else:
            print("✗ Version not accessible")
            
        # Test core classes
        core_classes = [
            'Schema', 'Catalog', 'Table', 'ReadBuilder', 
            'TableScan', 'Plan', 'RowType'
        ]
        
        missing_classes = []
        for cls_name in core_classes:
            if hasattr(pyfluss, cls_name):
                print(f"✓ {cls_name} accessible")
            else:
                missing_classes.append(cls_name)
                print(f"✗ {cls_name} not accessible")
        
        # Test JAR file
        package_dir = os.path.dirname(pyfluss.__file__)
        jar_dir = os.path.join(package_dir, 'jars')
        
        if os.path.exists(jar_dir):
            jar_files = [f for f in os.listdir(jar_dir) if f.endswith('.jar')]
            if jar_files:
                print(f"✓ JAR files found: {jar_files}")
            else:
                print("✗ No JAR files found")
        else:
            print("✗ JAR directory not found")
            
        if missing_classes:
            print(f"\nValidation completed with issues: {len(missing_classes)} missing classes")
            sys.exit(1)
        else:
            print("\n✓ All validation checks passed!")
            
    except ImportError as e:
        print(f"✗ Error importing pyfluss: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_example() -> None:
    """Show usage examples."""
    print("Pyfluss Usage Examples:")
    print("=" * 50)
    
    print("\n1. Basic catalog operations:")
    print("""
import pyfluss

# Create a catalog
config = {
    'fluss.connection.host': 'localhost',
    'fluss.connection.port': '9999'
}
catalog = pyfluss.Catalog.create(config)

# List databases
databases = catalog.list_databases()
print(f"Databases: {databases}")

# Get a table
table = catalog.get_table('my_database.my_table')
""")

    print("\n2. Reading data:")
    print("""
# Create a read builder
read_builder = table.new_read_builder()

# Build a scan with projection
scan = (read_builder
        .with_projection(['col1', 'col2'])
        .new_scan())

# Read data
plan = scan.plan()
for table_bucket in plan.table_buckets():
    reader = read_builder.new_read().create_reader(table_bucket)
    # Process data...
""")

    print("\n3. Writing data:")
    print("""
# Create a write builder and writer
write_builder = table.new_write_builder()
write = write_builder.new_write()

# Write records
for record in data:
    write.write(record)

# Close writer (data is automatically committed)
write.close()
""")


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog='pyfluss',
        description='Pyfluss - Fluss Python SDK command-line interface'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Version command
    subparsers.add_parser('version', help='Show version information')
    
    # Info command
    subparsers.add_parser('info', help='Show package information')
    
    # Validate command
    subparsers.add_parser('validate', help='Validate package installation')
    
    # Example command
    subparsers.add_parser('example', help='Show usage examples')
    
    args = parser.parse_args()
    
    if args.command == 'version':
        cmd_version()
    elif args.command == 'info':
        cmd_info()
    elif args.command == 'validate':
        cmd_validate()
    elif args.command == 'example':
        cmd_example()
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
