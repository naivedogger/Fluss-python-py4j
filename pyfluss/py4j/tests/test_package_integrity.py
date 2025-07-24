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

import unittest
import importlib
import sys
import os


class TestPackageIntegrity(unittest.TestCase):
    """Test package integrity and import structure."""

    def test_main_package_import(self):
        """Test that the main package can be imported."""
        import pyfluss
        self.assertIsNotNone(pyfluss)

    def test_version_attribute(self):
        """Test that version is accessible."""
        import pyfluss
        self.assertTrue(hasattr(pyfluss, '__version__'))
        self.assertIsInstance(pyfluss.__version__, str)
        self.assertRegex(pyfluss.__version__, r'^\d+\.\d+\.\d+.*')

    def test_api_modules_importable(self):
        """Test that API modules can be imported."""
        from pyfluss import api
        self.assertIsNotNone(api)

    def test_py4j_modules_importable(self):
        """Test that py4j modules can be imported."""
        from pyfluss import py4j
        self.assertIsNotNone(py4j)

    def test_core_classes_accessible(self):
        """Test that core classes are accessible from main package."""
        import pyfluss
        
        # Test that main classes are accessible
        core_classes = [
            'Schema', 'Catalog', 'Table', 'ReadBuilder', 
            'TableScan', 'Plan', 'RowType',
            'FlussTableRead', 'BatchWriteBuilder', 'BatchTableWrite',
            'FlussSchema'
        ]
        
        for cls_name in core_classes:
            self.assertTrue(hasattr(pyfluss, cls_name), 
                          f"Class {cls_name} not accessible from pyfluss")

    def test_jar_file_exists(self):
        """Test that required JAR file exists."""
        import pyfluss
        package_dir = os.path.dirname(pyfluss.__file__)
        jar_dir = os.path.join(package_dir, 'jars')
        
        self.assertTrue(os.path.exists(jar_dir), "JAR directory not found")
        
        jar_files = [f for f in os.listdir(jar_dir) if f.endswith('.jar')]
        self.assertGreater(len(jar_files), 0, "No JAR files found")

    def test_all_api_modules_exist(self):
        """Test that all expected API modules exist."""
        api_modules = [
            'catalog', 'table', 'read_builder', 'table_scan',
            'fluss_table_read', 'row_type',
            'write_builder', 'table_write', 'schema'
        ]
        
        for module_name in api_modules:
            module_path = f'pyfluss.api.{module_name}'
            try:
                importlib.import_module(module_path)
            except ImportError as e:
                self.fail(f"Could not import {module_path}: {e}")

    def test_py4j_modules_exist(self):
        """Test that py4j implementation modules exist."""
        py4j_modules = [
            'java_implementation', 'gateway_server', 'java_gateway'
        ]
        
        for module_name in py4j_modules:
            module_path = f'pyfluss.py4j.{module_name}'
            try:
                importlib.import_module(module_path)
            except ImportError as e:
                self.fail(f"Could not import {module_path}: {e}")

    def test_util_modules_exist(self):
        """Test that utility modules exist."""
        util_modules = [
            'constants', 'java_utils'
        ]
        
        for module_name in util_modules:
            module_path = f'pyfluss.py4j.util.{module_name}'
            try:
                importlib.import_module(module_path)
            except ImportError as e:
                self.fail(f"Could not import {module_path}: {e}")


class TestPackageConfiguration(unittest.TestCase):
    """Test package configuration and setup."""

    def test_python_version_compatibility(self):
        """Test Python version compatibility."""
        self.assertGreaterEqual(sys.version_info, (3, 8), 
                              "Python 3.8+ required")

    def test_package_metadata(self):
        """Test that package metadata is accessible."""
        import pyfluss
        
        # Test that basic metadata attributes exist
        self.assertTrue(hasattr(pyfluss, '__version__'))
        
        # Test version format
        version = pyfluss.__version__
        version_parts = version.split('.')
        self.assertGreaterEqual(len(version_parts), 3, 
                              "Version should have at least 3 parts")


if __name__ == '__main__':
    unittest.main()
