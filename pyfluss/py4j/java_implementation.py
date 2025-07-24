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
from typing import Dict, List, Optional, Any, Iterator, TYPE_CHECKING

# pyfluss.api implementation based on Java code & py4j lib

try:
    import pandas as pd
    import pyarrow as pa
except ImportError:
    pd = None
    pa = None

from pyfluss.py4j.java_gateway import get_gateway
from pyfluss.py4j.util import java_utils, constants
from pyfluss.py4j.util.java_utils import serialize_java_object, deserialize_java_object
from pyfluss.api import (
    catalog, table, read_builder, table_scan, row_type,
    fluss_table_read, write_builder, table_write
)

if TYPE_CHECKING:
    try:
        import duckdb.duckdb
        import ray
    except ImportError:
        pass


class Catalog(catalog.Catalog):
    """Fluss Catalog implementation using py4j."""

    def __init__(self, j_catalog, catalog_options: dict):
        self._j_catalog = j_catalog
        self._catalog_options = catalog_options

    @staticmethod
    def create(catalog_options: dict) -> 'Catalog':
        """Create a new catalog instance."""
        j_catalog_context = java_utils.to_j_catalog_context(catalog_options)
        gateway = get_gateway()
        j_catalog = gateway.jvm.org.example.FlussClientBridge.createCatalog(j_catalog_context)
        return Catalog(j_catalog, catalog_options)

    def get_table(self, identifier: str) -> 'Table':
        """Get a table by identifier."""
        j_identifier = java_utils.to_j_identifier(identifier)
        j_table = self._j_catalog.getTable(j_identifier)
        return Table(j_table, self._catalog_options)

    def create_database(self, name: str, ignore_if_exists: bool = False, 
                       properties: Optional[dict] = None):
        """Create a database."""
        if properties is None:
            properties = {}
        self._j_catalog.createDatabase(name, ignore_if_exists, properties)

    def create_table(self, identifier: str, schema: 'Schema', ignore_if_exists: bool = False):
        """Create a table with the given schema."""
        j_identifier = java_utils.to_j_identifier(identifier)
        j_schema = java_utils.to_fluss_schema(schema)
        self._j_catalog.createTable(j_identifier, j_schema, ignore_if_exists)


class Table(table.Table):
    """Fluss Table implementation using py4j."""

    def __init__(self, j_table, catalog_options: dict):
        self._j_table = j_table
        self._catalog_options = catalog_options

    def new_read_builder(self) -> 'ReadBuilder':
        """Create a new read builder for this table."""
        j_read_builder = get_gateway().jvm.org.example.FlussClientBridge.createReadBuilder(self._j_table)
        
        # Get primary keys
        if hasattr(self._j_table, 'primaryKeys') and not self._j_table.primaryKeys().isEmpty():
            primary_keys = [str(key) for key in self._j_table.primaryKeys()]
        else:
            primary_keys = None
        
        # Get partition keys  
        if hasattr(self._j_table, 'partitionKeys') and not self._j_table.partitionKeys().isEmpty():
            partition_keys = [str(key) for key in self._j_table.partitionKeys()]
        else:
            partition_keys = None
        
        return ReadBuilder(j_read_builder, self._j_table.rowType(),
                          self._catalog_options, primary_keys, partition_keys)

    def new_batch_write_builder(self) -> 'BatchWriteBuilder':
        """Create a new batch write builder for this table."""
        java_utils.check_batch_write(self._j_table)
        j_batch_write_builder = get_gateway().jvm.org.example.FlussClientBridge.createBatchWriteBuilder(self._j_table)
        return BatchWriteBuilder(j_batch_write_builder)


class ReadBuilder(read_builder.ReadBuilder):
    """Fluss ReadBuilder implementation using py4j."""

    def __init__(self, j_read_builder, j_row_type, catalog_options: dict, 
                 primary_keys: List[str], partition_keys: List[str]):
        self._j_read_builder = j_read_builder
        self._j_row_type = j_row_type
        self._catalog_options = catalog_options
        self._primary_keys = primary_keys
        self._partition_keys = partition_keys
        self._projection = None

    def with_projection(self, projection: List[str]) -> 'ReadBuilder':
        """Apply column projection to the read operation."""
        self._projection = projection
        field_names = list(map(lambda field: field.name(), self._j_row_type.getFields()))
        int_projection = list(map(lambda p: field_names.index(p), projection))
        gateway = get_gateway()
        int_projection_arr = gateway.new_array(gateway.jvm.int, len(projection))
        for i in range(len(projection)):
            int_projection_arr[i] = int_projection[i]
        self._j_read_builder.withProjection(int_projection_arr)
        return self

    def with_limit(self, limit: int) -> 'ReadBuilder':
        """Apply a limit to the read operation."""
        self._j_read_builder.withLimit(limit)
        return self

    def new_scan(self) -> 'TableScan':
        """Create a new table scan."""
        j_table_scan = self._j_read_builder.newScan()
        return TableScan(j_table_scan)

    def new_read(self) -> 'fluss_table_read.FlussTableRead':
        """Create a new Fluss table read."""
        j_table_read = self._j_read_builder.newRead().executeFilter()
        from pyfluss.api.fluss_table_read import FlussTableRead
        return FlussTableRead(j_table_read, self._j_read_builder.readType(),
                            self._catalog_options, self._projection,
                            self._primary_keys, self._partition_keys)

    def read_type(self) -> 'RowType':
        """Get the row type for the read operation."""
        return RowType(self._j_read_builder.readType())


class RowType(row_type.RowType):
    """Fluss RowType implementation using py4j."""

    def __init__(self, j_row_type):
        self._j_row_type = j_row_type

    def as_arrow(self):
        """Convert to Arrow schema."""
        return java_utils.to_arrow_schema(self._j_row_type)


class TableScan(table_scan.TableScan):
    """Fluss TableScan implementation using py4j."""

    def __init__(self, j_table_scan):
        self._j_table_scan = j_table_scan

    def plan(self) -> 'Plan':
        """Create an execution plan for the scan."""
        j_plan = self._j_table_scan.plan()
        j_table_buckets = j_plan.tableBuckets()
        return Plan(j_table_buckets)


class Plan(table_scan.Plan):
    """Fluss Plan implementation using py4j."""

    def __init__(self, j_table_buckets):
        self._j_table_buckets = j_table_buckets

    def table_buckets(self) -> List[Dict[str, Any]]:
        """Get the table buckets for this plan."""
        buckets = []
        for j_bucket in self._j_table_buckets:
            bucket_info = {
                'bucket_id': j_bucket.getBucketId() if hasattr(j_bucket, 'getBucketId') else 0,
                'partition': str(j_bucket.getPartition()) if hasattr(j_bucket, 'getPartition') else None,
                'metadata': serialize_java_object(j_bucket)
            }
            buckets.append(bucket_info)
        return buckets


class BatchWriteBuilder(write_builder.BatchWriteBuilder):
    """Fluss BatchWriteBuilder implementation using py4j."""

    def __init__(self, j_batch_write_builder):
        self._j_batch_write_builder = j_batch_write_builder

    def overwrite(self, static_partition: Optional[dict] = None) -> 'BatchWriteBuilder':
        """Configure the write to overwrite existing data."""
        if static_partition is None:
            static_partition = {}
        self._j_batch_write_builder.withOverwrite(static_partition)
        return self

    def new_write(self) -> 'BatchTableWrite':
        """Create a new batch table write."""
        j_batch_table_write = self._j_batch_write_builder.newWrite()
        return BatchTableWrite(j_batch_table_write, self._j_batch_write_builder.rowType())


class BatchTableWrite(table_write.BatchTableWrite):
    """Fluss BatchTableWrite implementation using py4j."""

    def __init__(self, j_batch_table_write, j_row_type):
        self._j_batch_table_write = j_batch_table_write
        self._j_bytes_writer = get_gateway().jvm.org.example.FlussDataWriter.createBytesWriter(
            j_batch_table_write, j_row_type)
        
        if pa:
            self._arrow_schema = java_utils.to_arrow_schema(j_row_type)

    def write_arrow(self, table):
        """Write an Arrow table."""
        if not pa:
            raise ImportError("PyArrow is required for Arrow table writing")
            
        for record_batch in table.to_reader():
            self._write_arrow_batch(record_batch)

    def write_arrow_batch(self, record_batch):
        """Write an Arrow record batch."""
        self._write_arrow_batch(record_batch)

    def write_pandas(self, dataframe):
        """Write a Pandas DataFrame."""
        if not pd or not pa:
            raise ImportError("Pandas and PyArrow are required for DataFrame writing")
            
        record_batch = pa.RecordBatch.from_pandas(dataframe, schema=self._arrow_schema)
        self._write_arrow_batch(record_batch)

    def _write_arrow_batch(self, record_batch):
        """Write an Arrow record batch to the underlying writer."""
        if not pa:
            raise ImportError("PyArrow is required for Arrow batch writing")
            
        stream = pa.BufferOutputStream()
        with pa.RecordBatchStreamWriter(stream, record_batch.schema) as writer:
            writer.write(record_batch)
        arrow_bytes = stream.getvalue().to_pybytes()
        self._j_bytes_writer.write(arrow_bytes)

    def close(self):
        """Close the write operation."""
        self._j_batch_table_write.close()
        self._j_bytes_writer.close()


class FlussTableRead(fluss_table_read.FlussTableRead):
    """Fluss-native table read implementation using py4j."""

    def __init__(self, j_table_read, j_read_type, catalog_options, 
                 projection, primary_keys: List[str], partition_keys: List[str]):
        self._j_table_read = j_table_read
        self._j_read_type = j_read_type
        self._catalog_options = catalog_options
        self._projection = projection
        self._primary_keys = primary_keys
        self._partition_keys = partition_keys
        
        if pa:
            self._arrow_schema = java_utils.to_arrow_schema(j_read_type)
        
        # Create bytes reader for parallel processing
        self._j_bytes_reader = get_gateway().jvm.org.example.FlussDataReader.createParallelBytesReader(
            j_table_read, j_read_type, FlussTableRead._get_max_workers(catalog_options))

    def to_arrow_from_buckets(self, table_buckets: List[Dict[str, Any]]):
        """Convert table buckets to Arrow table."""
        if not pa:
            raise ImportError("PyArrow is required for Arrow conversion")
            
        record_batch_reader = self.to_arrow_batch_reader_from_buckets(table_buckets)
        return pa.Table.from_batches(record_batch_reader, schema=self._arrow_schema)

    def to_arrow_batch_reader_from_buckets(self, table_buckets: List[Dict[str, Any]]):
        """Convert table buckets to Arrow batch reader."""
        if not pa:
            raise ImportError("PyArrow is required for Arrow batch reader")
            
        j_buckets = []
        for bucket in table_buckets:
            j_bucket = deserialize_java_object(bucket['metadata'])
            j_buckets.append(j_bucket)
            
        self._j_bytes_reader.setTableBuckets(j_buckets)
        batch_iterator = self._batch_generator()
        return pa.RecordBatchReader.from_batches(self._arrow_schema, batch_iterator)

    def to_pandas_from_buckets(self, table_buckets: List[Dict[str, Any]]):
        """Convert table buckets to Pandas DataFrame."""
        if not pd:
            raise ImportError("Pandas is required for Pandas conversion")
        return self.to_arrow_from_buckets(table_buckets).to_pandas()

    def read_batches(self, table_buckets: List[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
        """Read data in batches from table buckets."""
        for bucket in table_buckets:
            j_bucket = deserialize_java_object(bucket['metadata'])
            j_reader = get_gateway().jvm.org.example.FlussDataReader.createBucketReader(
                self._j_table_read, j_bucket)
            
            while j_reader.hasNext():
                record = j_reader.next()
                yield self._convert_record_to_dict(record)

    def _convert_record_to_dict(self, j_record) -> Dict[str, Any]:
        """Convert Java record to Python dictionary."""
        # This would need proper implementation based on the record format
        # For now, return a simple placeholder
        return {"placeholder": "record_data"}

    @staticmethod
    def _get_max_workers(catalog_options):
        """Get maximum number of workers from catalog options."""
        max_workers = int(catalog_options.get(constants.MAX_WORKERS, 1))
        if max_workers <= 0:
            raise ValueError("max_workers must be greater than 0")
        return max_workers

    def _batch_generator(self) -> Iterator:
        """Generate batches from the bytes reader."""
        if not pa:
            raise ImportError("PyArrow is required for batch generation")
            
        while True:
            next_bytes = self._j_bytes_reader.next()
            if next_bytes is None:
                break
            else:
                stream_reader = pa.RecordBatchStreamReader(pa.BufferReader(next_bytes))
                yield from stream_reader


class FlussSchema:
    """Fluss Schema implementation that wraps SchemaUtil.java functionality."""
    
    def __init__(self, j_schema):
        self._j_schema = j_schema
        self._schema_map = None
        self._field_info_cache = {}

    def _get_schema_map(self) -> Dict[str, Any]:
        """Get schema map using SchemaUtil.schemaToMap."""
        if self._schema_map is None:
            gateway = get_gateway()
            self._schema_map = gateway.jvm.org.example.SchemaUtil.schemaToMap(self._j_schema)
        return self._schema_map

    def get_field_names(self) -> List[str]:
        """Returns a list of field names in the schema."""
        schema_map = self._get_schema_map()
        return list(schema_map.get('field_names', []))

    def get_field_types(self) -> Dict[str, str]:
        """Returns a dictionary mapping field names to their types."""
        schema_map = self._get_schema_map()
        field_types = {}
        
        fields = schema_map.get('fields', [])
        for field in fields:
            field_types[field.get('name')] = field.get('type')
        
        return field_types

    def get_primary_keys(self) -> Optional[List[str]]:
        """Returns the primary key field names if any."""
        schema_map = self._get_schema_map()
        primary_keys = schema_map.get('primary_key')
        return list(primary_keys) if primary_keys else None

    def to_dict(self) -> Dict[str, Any]:
        """Converts the schema to a dictionary representation."""
        return self._get_schema_map()

    def get_field_info(self, field_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific field."""
        if field_name not in self._field_info_cache:
            gateway = get_gateway()
            field_info = gateway.jvm.org.example.SchemaUtil.getFieldInfo(self._j_schema, field_name)
            self._field_info_cache[field_name] = dict(field_info)
        
        return self._field_info_cache[field_name]

    def validate(self) -> Dict[str, Any]:
        """Validate the schema and return validation results."""
        gateway = get_gateway()
        validation_result = gateway.jvm.org.example.SchemaUtil.validateSchema(self._j_schema)
        return dict(validation_result)
