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

import pickle
import pyarrow as pa
from typing import Dict, List, Any, Optional
from py4j.java_gateway import JavaGateway

from pyfluss.py4j.java_gateway import get_gateway


def serialize_java_object(j_object):
    """Serialize a Java object to bytes using py4j."""
    gateway = get_gateway()
    byte_array = gateway.jvm.org.apache.commons.lang3.SerializationUtils.serialize(j_object)
    return bytes(byte_array)


def deserialize_java_object(byte_data):
    """Deserialize bytes back to a Java object using py4j."""
    gateway = get_gateway()
    byte_array = gateway.new_array(gateway.jvm.byte, len(byte_data))
    for i, b in enumerate(byte_data):
        byte_array[i] = b
    return gateway.jvm.org.apache.commons.lang3.SerializationUtils.deserialize(byte_array)


def to_j_identifier(identifier: str):
    """Convert Python string identifier to Java Identifier object."""
    gateway = get_gateway()
    parts = identifier.split('.')
    if len(parts) == 1:
        # Table name only
        return gateway.jvm.org.example.CatalogFactory.MockIdentifier.of(parts[0])
    elif len(parts) == 2:
        # Database.table
        return gateway.jvm.org.example.CatalogFactory.MockIdentifier.of(parts[0], parts[1])
    else:
        raise ValueError(f"Invalid identifier format: {identifier}")


def to_j_catalog_context(catalog_options: Dict[str, Any]):
    """Convert Python catalog options to Java CatalogContext."""
    gateway = get_gateway()
    j_options = gateway.jvm.java.util.HashMap()
    
    for key, value in catalog_options.items():
        j_options.put(str(key), str(value))
    
    return gateway.jvm.org.example.CatalogFactory.MockCatalogContext.create(j_options)


def to_fluss_schema(schema_dict: Dict[str, Any]):
    """Convert Python schema dictionary to Fluss Schema object."""
    gateway = get_gateway()
    
    # This is a simplified implementation
    # You would need to implement based on your actual Fluss Schema API
    schema_builder = gateway.jvm.com.alibaba.fluss.metadata.Schema.newBuilder()
    
    for field_name, field_type in schema_dict.get('fields', {}).items():
        # Add fields to schema builder
        # This needs to be implemented based on actual Fluss API
        pass
    
    return schema_builder.build()


def to_arrow_schema(j_row_type):
    """Convert Java RowType to PyArrow Schema."""
    gateway = get_gateway()
    
    # Get field information from Java RowType
    field_names = j_row_type.getFieldNames()
    field_types = j_row_type.getChildren()
    
    arrow_fields = []
    for i in range(len(field_names)):
        field_name = field_names[i]
        j_field_type = field_types[i]
        
        # Convert Java DataType to Arrow DataType
        arrow_type = _java_type_to_arrow_type(j_field_type)
        arrow_fields.append(pa.field(field_name, arrow_type))
    
    return pa.schema(arrow_fields)


def _java_type_to_arrow_type(j_data_type):
    """Convert Java DataType to Arrow DataType."""
    type_str = str(j_data_type).lower()
    
    if 'int' in type_str:
        return pa.int32()
    elif 'bigint' in type_str:
        return pa.int64()
    elif 'string' in type_str or 'varchar' in type_str:
        return pa.string()
    elif 'double' in type_str:
        return pa.float64()
    elif 'float' in type_str:
        return pa.float32()
    elif 'boolean' in type_str:
        return pa.bool_()
    elif 'timestamp' in type_str:
        return pa.timestamp('us')
    elif 'date' in type_str:
        return pa.date32()
    elif 'decimal' in type_str:
        return pa.decimal128(38, 18)  # Default precision and scale
    elif 'bytes' in type_str or 'binary' in type_str:
        return pa.binary()
    else:
        # Default to string for unknown types
        return pa.string()


def check_batch_write(j_table):
    """Check if the table supports batch write operations."""
    # Implement based on your Fluss table API
    return True
