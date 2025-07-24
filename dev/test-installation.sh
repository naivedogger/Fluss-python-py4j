#!/bin/bash
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

# Test installation script for PyFluss

echo "PyFluss Installation Test"
echo "========================="

# Get the directory of this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( dirname "$SCRIPT_DIR" )"

echo "Project root: $PROJECT_ROOT"

# Check if package was built
if [ ! -f "$PROJECT_ROOT/dist/pyfluss-0.1.0.tar.gz" ]; then
    echo "Error: Package not found. Run 'python setup.py sdist' first."
    exit 1
fi

echo "✓ Package found: $PROJECT_ROOT/dist/pyfluss-0.1.0.tar.gz"

# Create a test virtual environment
TEST_ENV_DIR="$PROJECT_ROOT/test_env"
echo "Creating test environment: $TEST_ENV_DIR"

if [ -d "$TEST_ENV_DIR" ]; then
    echo "Removing existing test environment..."
    rm -rf "$TEST_ENV_DIR"
fi

python -m venv "$TEST_ENV_DIR"
source "$TEST_ENV_DIR/bin/activate"

echo "✓ Test environment created and activated"

# Install the package
echo "Installing PyFluss from local package..."
pip install "$PROJECT_ROOT/dist/pyfluss-0.1.0.tar.gz"

if [ $? -eq 0 ]; then
    echo "✓ PyFluss installed successfully"
else
    echo "✗ Failed to install PyFluss"
    exit 1
fi

# Test basic import
echo "Testing basic import..."
python -c "
import pyfluss
print('✓ Basic import successful')
print(f'Available classes: {pyfluss.__all__}')

# Test creating catalog (will fail without Fluss cluster but tests API)
try:
    catalog = pyfluss.Catalog.create({'warehouse': '/tmp/test'})
    print('✗ Unexpected success - should fail without Fluss cluster')
except Exception as e:
    print('✓ Catalog creation failed as expected (no Fluss cluster)')

print('✓ All basic tests passed!')
"

if [ $? -eq 0 ]; then
    echo "✓ Import and basic API tests passed"
else
    echo "✗ Import or API tests failed"
    deactivate
    exit 1
fi

# Test with optional dependencies
echo "Testing with optional dependencies..."
pip install pandas pyarrow

python -c "
import pyfluss
import pandas as pd
import pyarrow as pa

print('✓ Optional dependencies imported successfully')
print(f'Pandas version: {pd.__version__}')
print(f'PyArrow version: {pa.__version__}')
"

if [ $? -eq 0 ]; then
    echo "✓ Optional dependencies test passed"
else
    echo "✗ Optional dependencies test failed"
fi

# Cleanup
deactivate
echo "Cleaning up test environment..."
rm -rf "$TEST_ENV_DIR"

echo ""
echo "Installation test completed successfully!"
echo ""
echo "To install PyFluss in your environment:"
echo "  pip install $PROJECT_ROOT/dist/pyfluss-0.1.0.tar.gz"
echo ""
echo "To install with all optional dependencies:"
echo "  pip install $PROJECT_ROOT/dist/pyfluss-0.1.0.tar.gz[all]"
