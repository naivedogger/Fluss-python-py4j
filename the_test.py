#!/usr/bin/env python3
"""
PyFluss comprehensive test script with Schema support.
"""

import pandas as pd
import pyarrow as pa
from pyfluss import connect
from pyfluss.api import Schema, DatabaseDescriptor

# Connect to Fluss
conn = connect("localhost:9123")

# Get admin instance
admin = conn.getAdmin()

# Create database using DatabaseDescriptor
db_descriptor = DatabaseDescriptor.builder().comment("Test database").build()
admin.create_database("tmp_db", db_descriptor, if_not_exists=True)

# Create PyArrow schema
pa_schema = pa.schema([
    ('date', pa.string()),
    ('hour', pa.string()),
    ('key', pa.int64()),
    ('value', pa.string())
])

# Convert to Fluss schema and add primary key
fluss_schema = Schema.from_arrow_schema(pa_schema)
schema_with_pk = fluss_schema.with_primary_keys(['key'])

# Create table using Schema object
result = conn.create_table("tmp_db", "test_table", schema_with_pk, if_not_exists=True)

# Verify table creation
tables = conn.list_tables("tmp_db")
print(f"Tables in tmp_db: {tables}")

# Get table schema info
schema_info = conn.get_table_schema("tmp_db", "test_table")
if schema_info:
    print("Table schema retrieved:")
    for col in schema_info['columns']:
        print(f"  - {col['name']}: {col['type']}")
else:
    print("Failed to get table schema")

# Create writer and write data
writer = conn.create_writer("tmp_db.test_table")

# Write test data
test_data = [
    {'date': '2023-10-01', 'hour': '12', 'key': 1, 'value': 'test1'},
    {'date': '2023-10-01', 'hour': '13', 'key': 2, 'value': 'test2'},
    {'date': '2023-10-01', 'hour': '14', 'key': 3, 'value': 'test-3'}
]

# 目前都是 write_row，实际上都是 upsert
# 应该根据表实际的类型选择是 upsert 还是 append
for record in test_data:
    writer.write_row(record)

# 这个 close 目前也不干任何事情
writer.close()

# Create reader and read data
reader = conn.create_reader("tmp_db.test_table")

# Read and display data
print("\n Reading data from table:")
try:
    # 而且目前 reader 也都是 batch_reader
    data = reader.read_rows(limit=10)
    if data:
        for i, record in enumerate(data):
            print(f"Record {i+1}: {record}")
        print(f"Successfully read {len(data)} records")
    else:
        print("No data found in table")
except Exception as e:
    print(f"Error reading data: {e}")
    import traceback
    traceback.print_exc()

reader.close()

conn.drop_table("tmp_db", "test_table", if_exists=True)
conn.drop_database("tmp_db", if_exists=True)

conn.close()