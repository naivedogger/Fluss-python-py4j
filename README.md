# PyFluss - Python SDK for Apache Fluss

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://img.shields.io/badge/build-passing-green.svg)](#)

PyFluss is a comprehensive Python SDK for [Apache Fluss](https://fluss.apache.org), providing native Python interfaces to interact with Fluss's real-time data lake storage. Built with modern Python best practices and featuring efficient Py4J-based Java bridge for optimal performance.

## 🚀 Features

- **🔌 Native Fluss Integration**: Direct integration with Apache Fluss using TableBucket architecture
- **🐍 Pythonic API**: Clean, intuitive Python interfaces with type hints and modern async support
- **⚡ High Performance**: Efficient Py4J bridge for seamless Python-Java interoperability
- **📊 Data Ecosystem**: Built-in support for pandas, PyArrow, and DuckDB integration
- **🛡️ Production Ready**: Comprehensive error handling, connection management, and testing
- **🔧 Developer Friendly**: Easy setup, comprehensive documentation, and debugging tools

## 📦 Installation

### From Source

```bash
git clone https://github.com/your-org/fluss-python-py4j.git
cd fluss-python-py4j
pip install .
```

### Development Installation

```bash
# Clone repository
git clone https://github.com/your-org/fluss-python-py4j.git
cd fluss-python-py4j

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# .venv\Scripts\activate  # Windows

# Install in development mode
pip install -e ".[dev]"
```

### Optional Dependencies

```bash
# For pandas support
pip install ".[pandas]"

# For PyArrow support  
pip install ".[arrow]"

# For all integrations
pip install ".[all]"
```

## 🎯 Quick Start

### Basic Usage

```python
import pyfluss

# Create connection to Fluss
connection = pyfluss.connect(
    host='localhost',
    port=9123
)

# Get catalog and table
catalog = connection.catalog
table = catalog.get_table('my_database.my_table')

# Read data using Fluss-native TableBucket approach
read_builder = table.new_read_builder()
table_scan = read_builder.new_scan()
plan = table_scan.plan()

# Process table buckets (Fluss's native partitioning)
for bucket in plan.table_buckets():
    reader = read_builder.new_read()
    data = reader.read_batches([bucket])
    for record in data:
        print(record)

# Write data
write_builder = table.new_batch_write_builder()
writer = write_builder.new_write()

# Write records and commit
import pandas as pd
df = pd.DataFrame({'id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie']})
writer.write_pandas(df)
commit_messages = writer.prepare_commit()

committer = write_builder.new_commit()
committer.commit(commit_messages)

# Close connection
connection.close()
```

### Advanced Data Processing

```python
import pyfluss
import pandas as pd

# Connect to Fluss
conn = pyfluss.connect('localhost:9123')
table = conn.catalog.get_table('analytics.user_events')

# Create filtered read with projection
read_builder = (table.new_read_builder()
    .with_filter(pyfluss.filters.greater_than('timestamp', '2024-01-01'))
    .with_projection(['user_id', 'event_type', 'timestamp'])
    .with_limit(1000))

# Read to pandas DataFrame
df = read_builder.to_pandas()
print(f"Read {len(df)} records")

# Process with PyArrow for better performance
arrow_table = read_builder.to_arrow()
processed_data = arrow_table.to_pandas().groupby('event_type').size()
print(processed_data)
```

## 🏗️ Architecture

### Project Structure

```
pyfluss/
├── __init__.py              # Main exports and high-level API
├── version.py               # Version management
├── connection.py            # Connection management
├── reader.py               # Data reading utilities
├── writer.py               # Data writing utilities
├── cli.py                  # Command-line interface
├── api/                    # Core API abstractions
│   ├── catalog.py          # Catalog operations
│   ├── table.py            # Table interface
│   ├── read_builder.py     # Read operations builder
│   ├── write_builder.py    # Write operations builder
│   ├── fluss_table_read.py # Fluss-native table reading
│   ├── table_scan.py       # Table scanning with TableBuckets
│   ├── predicate.py        # Query predicates and filters
│   ├── schema.py           # Schema definitions
│   └── ...
├── py4j/                   # Java bridge implementation
│   ├── java_implementation.py  # Core Py4J bridge
│   ├── java_gateway.py     # Gateway management
│   ├── gateway_server.py   # Gateway server
│   ├── util/               # Utilities and helpers
│   └── tests/              # Integration tests
└── jars/                   # Java dependencies
    └── fluss-python-py4j-*.jar
```

### Fluss Integration

PyFluss is built specifically for Apache Fluss and uses Fluss's native concepts:

- **TableBucket**: Fluss's partitioning mechanism (not Paimon's Split concept)
- **Native Streaming**: Direct integration with Fluss's streaming capabilities
- **Real-time Processing**: Support for Fluss's real-time data processing
- **Efficient Serialization**: Optimized data transfer using Fluss formats

## 🛠️ Development

### Prerequisites

- **Python 3.8+**
- **Java 11+** (recommended) or Java 8+
- **Maven 3.6+**
- **Apache Fluss** (for testing)

### Development Setup

```bash
# Clone repository
git clone https://github.com/your-org/fluss-python-py4j.git
cd fluss-python-py4j

# Setup Python environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e ".[dev,test,arrow,pandas]"

# Build Java components
mvn clean package -DskipTests

# Copy JAR to Python package
cp target/fluss-python-py4j-*.jar pyfluss/jars/
```

### Running Tests

```bash
# Run complete test suite
python test_complete_workflow.py

# Run unit tests
python -m pytest pyfluss/py4j/tests/ -v

# Run with coverage
python -m pytest pyfluss/py4j/tests/ --cov=pyfluss --cov-report=html

# Lint and type checking
flake8 pyfluss/
mypy pyfluss/
```

### Building and Packaging

```bash
# Build package using our script
./build_package.sh

# Manual build
python -m build

# Install locally
pip install dist/pyfluss-*.whl
```

## 📚 API Reference

### Connection Management

```python
import pyfluss

# Direct connection
conn = pyfluss.connect('localhost:9123')

# Connection with configuration
conn = pyfluss.connect({
    'host': 'localhost',
    'port': 9123,
    'timeout': 30,
    'max_retries': 3
})

# Using catalog directly
catalog = pyfluss.Catalog.create({
    'fluss.connection.host': 'localhost',
    'fluss.connection.port': '9123'
})
```

### Catalog Operations

```python
# List databases
databases = catalog.list_databases()

# Database operations
catalog.create_database('analytics', ignore_if_exists=True)
database = catalog.get_database('analytics')

# Table operations
tables = catalog.list_tables('analytics')
table = catalog.get_table('analytics.user_events')

# Schema information
schema = table.schema()
print(f"Fields: {schema.field_names}")
print(f"Primary keys: {schema.primary_keys}")
```

### Reading Data

```python
# Basic reading
table = catalog.get_table('db.table')
read_builder = table.new_read_builder()

# Apply filters and projections
filtered_reader = (read_builder
    .with_filter(pyfluss.filters.equal('status', 'active'))
    .with_projection(['id', 'name', 'created_at'])
    .with_limit(1000))

# Read using table buckets (Fluss native)
scan = filtered_reader.new_scan()
plan = scan.plan()
table_buckets = plan.table_buckets()

# Convert to different formats
fluss_reader = filtered_reader.new_read()
df = fluss_reader.to_pandas_from_buckets(table_buckets)
arrow_table = fluss_reader.to_arrow_from_buckets(table_buckets)

# Stream processing
for record_batch in fluss_reader.read_batches(table_buckets):
    # Process each batch
    process_batch(record_batch)
```

### Writing Data

```python
# Batch writing
table = catalog.get_table('db.table')
write_builder = table.new_batch_write_builder()

# Configure write options
writer = write_builder.overwrite().new_write()

# Write pandas DataFrame
import pandas as pd
df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'score': [95.5, 87.2, 92.8]
})
writer.write_pandas(df)

# Write PyArrow table
import pyarrow as pa
table_data = pa.table({
    'id': [4, 5, 6],
    'name': ['David', 'Eve', 'Frank'],
    'score': [88.1, 94.3, 90.7]
})
writer.write_arrow(table_data)

# Commit changes
commit_messages = writer.prepare_commit()
committer = write_builder.new_commit()
committer.commit(commit_messages)
writer.close()
```

### PyArrow Schema Integration

PyFluss provides seamless integration with PyArrow schemas for efficient data processing:

```python
import pyarrow as pa
import pyfluss
from pyfluss.api import Schema, ArrowSchema

# Create a PyArrow schema
arrow_schema = pa.schema([
    pa.field('customer_id', pa.int64(), nullable=False),
    pa.field('order_id', pa.string(), nullable=False),
    pa.field('product_name', pa.string(), nullable=False),
    pa.field('quantity', pa.int32(), nullable=False),
    pa.field('unit_price', pa.decimal128(10, 2), nullable=False),
    pa.field('order_date', pa.timestamp('us'), nullable=False),
    pa.field('is_premium', pa.bool_(), nullable=False)
])

# Convert to Fluss schema
fluss_schema = Schema.from_arrow_schema(arrow_schema)
print(f"Converted schema with {fluss_schema.get_field_count()} fields")
print(f"Field types: {fluss_schema.get_field_types()}")

# Add primary keys
schema_with_pk = fluss_schema.with_primary_keys(['customer_id', 'order_id'])
print(f"Primary keys: {schema_with_pk.get_primary_keys()}")

# Create a subset for analytics
analytics_schema = fluss_schema.select(['customer_id', 'product_name', 'quantity', 'unit_price'])
print(f"Analytics fields: {analytics_schema.get_field_names()}")

# Convert back to PyArrow for data processing
back_to_arrow = fluss_schema.to_arrow_schema()
assert arrow_schema.equals(back_to_arrow)

# Schema validation
validation = fluss_schema.validate()
print(f"Schema valid: {validation['valid']}")

# Create schema from dictionary
schema_dict = {
    'fields': [
        {'name': 'id', 'type': 'int64', 'nullable': False},
        {'name': 'name', 'type': 'string', 'nullable': True},
        {'name': 'score', 'type': 'float64', 'nullable': True}
    ],
    'primary_key': ['id']
}

from pyfluss.api import create_schema_from_dict
dict_schema = create_schema_from_dict(schema_dict)
print(f"Schema from dict: {dict_schema.get_field_types()}")
```

## ⚙️ Configuration

### Connection Configuration

```python
# Basic connection
config = {
    'host': 'localhost',
    'port': 9123,
    'timeout': 30,
    'database': 'default'
}

# Advanced Fluss configuration
config = {
    'fluss.connection.host': 'localhost',
    'fluss.connection.port': '9123',
    'fluss.connection.timeout': '30s',
    'fluss.connection.retry.max': '3',
    'fluss.client.buffer.size': '64MB',
    'fluss.client.batch.size': '1000'
}
```

### Py4J Gateway Configuration

```python
# Gateway settings
config = {
    'py4j.gateway.port': 25333,
    'py4j.gateway.auto_start': True,
    'py4j.java.path': '/usr/bin/java',
    'py4j.max.workers': 4
}
```

### Performance Tuning

```python
# For large data processing
config = {
    'fluss.read.batch.size': 10000,
    'fluss.write.buffer.size': '128MB',
    'py4j.max.workers': 8,
    'arrow.buffer.size': '256MB'
}
```

## 🎯 Examples

### Complete Workflow Example

```python
import pyfluss
import pandas as pd

# 1. Connect to Fluss
conn = pyfluss.connect('localhost:9123')

# 2. Setup database and table
catalog = conn.catalog
catalog.create_database('analytics', ignore_if_exists=True)

# 3. Create sample data
users_data = pd.DataFrame({
    'user_id': range(1, 101),
    'username': [f'user_{i:03d}' for i in range(1, 101)],
    'email': [f'user{i:03d}@example.com' for i in range(1, 101)],
    'age': [20 + (i % 50) for i in range(1, 101)],
    'city': ['Beijing', 'Shanghai', 'Guangzhou', 'Shenzhen'] * 25
})

# 4. Write data
table = catalog.get_table('analytics.users')
write_builder = table.new_batch_write_builder()
writer = write_builder.new_write()
writer.write_pandas(users_data)

# Commit the write
commit_messages = writer.prepare_commit()
committer = write_builder.new_commit()
committer.commit(commit_messages)
writer.close()

# 5. Read and analyze data
read_builder = table.new_read_builder()
filtered_data = (read_builder
    .with_filter(pyfluss.filters.greater_than('age', 30))
    .with_projection(['user_id', 'username', 'city'])
    .to_pandas())

print(f"Found {len(filtered_data)} users over 30")
print(filtered_data.head())

# 6. Close connection
conn.close()
```

### Real-time Processing Example

```python
import pyfluss

def process_streaming_data():
    conn = pyfluss.connect('localhost:9123')
    table = conn.catalog.get_table('events.user_actions')
    
    # Create streaming reader
    read_builder = table.new_read_builder()
    reader = read_builder.new_read()
    
    # Process in real-time
    scan = read_builder.new_scan()
    plan = scan.plan()
    
    for bucket in plan.table_buckets():
        for batch in reader.read_batches([bucket]):
            # Process each batch in real-time
            process_user_actions(batch)
            
    conn.close()

def process_user_actions(batch):
    # Your real-time processing logic here
    for record in batch:
        print(f"Processing action: {record}")
```

## 🤝 Contributing

We welcome contributions to PyFluss! Here's how to get started:

### Development Workflow

1. **Fork the repository**

   ```bash
   git clone https://github.com/your-username/fluss-python-py4j.git
   cd fluss-python-py4j
   ```

2. **Set up development environment**

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -e ".[dev,test]"
   ```

3. **Make your changes**
   - Add new features or fix bugs
   - Write tests for your changes
   - Update documentation as needed

4. **Test your changes**

   ```bash
   python test_complete_workflow.py
   python -m pytest pyfluss/py4j/tests/
   ```

5. **Submit a pull request**
   - Create a clear description of your changes
   - Reference any related issues
   - Ensure all tests pass

### Code Standards

- Follow PEP 8 for Python code style
- Add type hints for all public APIs
- Write docstrings for all public functions
- Maintain test coverage above 80%

## 🐛 Troubleshooting

### Common Issues

#### Connection Failed

```python
# Check if Fluss server is running
# Verify host and port configuration
conn = pyfluss.connect('localhost:9123', timeout=60)
```

#### Java Gateway Issues

```bash
# Ensure Java is installed and accessible
java -version

# Check if gateway port is available
netstat -an | grep 25333
```

#### Memory Issues with Large Data

```python
# Process data in smaller batches
read_builder = table.new_read_builder().with_limit(10000)
# Increase JVM memory: -Xmx4g
```

### Debug Mode

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Enable PyFluss debug logging
pyfluss.set_log_level('DEBUG')
```

## 📄 License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.

## 🔗 Links

- **Documentation**: [Apache Fluss Documentation](https://fluss.apache.org)
- **Issues**: [GitHub Issues](https://github.com/your-org/fluss-python-py4j/issues)
- **Discussions**: [GitHub Discussions](https://github.com/your-org/fluss-python-py4j/discussions)

## 🙏 Acknowledgments

This project is inspired by:

- [Apache Fluss](https://github.com/apache/fluss)

---

Built with ❤️ for the Apache Fluss community
