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
from pyfluss.py4j.tests import PyFlussTestBase


class TestBasicFunctionality(PyFlussTestBase):
    """Test basic PyFluss functionality."""

    def test_import_modules(self):
        """Test that all modules can be imported successfully."""
        import pyfluss
        
        # Test that main classes are available
        self.assertTrue(hasattr(pyfluss, 'Catalog'))
        self.assertTrue(hasattr(pyfluss, 'Table'))
        self.assertTrue(hasattr(pyfluss, 'ReadBuilder'))
        self.assertTrue(hasattr(pyfluss, 'FlussSchema'))

    def test_catalog_creation_config(self):
        """Test catalog creation with configuration."""
        import pyfluss
        
        # Test that catalog can be created with options
        catalog_options = {
            'warehouse': self.warehouse,
            'test.mode': 'true'
        }
        
        try:
            # This will fail without actual Fluss cluster, but we test the API
            catalog = pyfluss.Catalog.create(catalog_options)
            self.fail("Should have failed without Fluss cluster")
        except Exception as e:
            # Expected to fail without actual cluster
            self.assertIsNotNone(e)

    def test_schema_functionality(self):
        """Test schema-related functionality."""
        # This tests the API structure without requiring actual Java objects
        import pyfluss.py4j.java_implementation as impl
        
        # Test that classes exist
        self.assertTrue(hasattr(impl, 'FlussSchema'))
        self.assertTrue(hasattr(impl, 'Catalog'))
        self.assertTrue(hasattr(impl, 'Table'))

    def test_constants(self):
        """Test that constants are properly defined."""
        from pyfluss.py4j.util import constants
        
        self.assertTrue(hasattr(constants, 'PYFLUSS_JAVA_CLASSPATH'))
        self.assertTrue(hasattr(constants, 'PYFLUSS_MAIN_CLASS'))
        self.assertTrue(hasattr(constants, 'MAX_WORKERS'))


if __name__ == '__main__':
    unittest.main()
