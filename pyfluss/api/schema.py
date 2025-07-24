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

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any


class Schema(ABC):
    """
    Abstract base class for Fluss Schema.
    Provides interface for schema operations and conversions.
    """
    
    @abstractmethod
    def get_field_names(self) -> List[str]:
        """
        Returns a list of field names in the schema.
        
        Returns:
            List of field names
        """
        pass
    
    @abstractmethod
    def get_field_types(self) -> Dict[str, str]:
        """
        Returns a dictionary mapping field names to their types.
        
        Returns:
            Dictionary of field name to type mappings
        """
        pass
    
    @abstractmethod
    def get_primary_keys(self) -> Optional[List[str]]:
        """
        Returns the primary key field names if any.
        
        Returns:
            List of primary key field names or None
        """
        pass
    
    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the schema to a dictionary representation.
        
        Returns:
            Dictionary representation of the schema
        """
        pass

    @abstractmethod
    def get_field_info(self, field_name: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific field.
        
        Args:
            field_name: Name of the field to get info for
            
        Returns:
            Dictionary containing field information
        """
        pass

    @abstractmethod
    def validate(self) -> Dict[str, Any]:
        """
        Validate the schema and return validation results.
        
        Returns:
            Dictionary containing validation results and warnings
        """
        pass

    def get_field_count(self) -> int:
        """
        Get the number of fields in the schema.
        
        Returns:
            Number of fields
        """
        return len(self.get_field_names())

    def has_field(self, field_name: str) -> bool:
        """
        Check if the schema contains a specific field.
        
        Args:
            field_name: Name of the field to check
            
        Returns:
            True if field exists, False otherwise
        """
        return field_name in self.get_field_names()

    @classmethod
    def from_arrow_schema(cls, arrow_schema) -> 'Schema':
        """
        Create a Schema from a PyArrow schema.
        
        Args:
            arrow_schema: PyArrow schema object
            
        Returns:
            Schema instance created from the Arrow schema
            
        Raises:
            ImportError: If PyArrow is not available
            ValueError: If the Arrow schema is invalid
        """
        try:
            import pyarrow as pa
        except ImportError:
            raise ImportError("PyArrow is required to create schema from Arrow schema")
        
        if not isinstance(arrow_schema, pa.Schema):
            raise ValueError("Input must be a PyArrow Schema object")
        
        return ArrowSchema(arrow_schema)
    
    @abstractmethod
    def to_arrow_schema(self):
        """
        Convert this schema to a PyArrow schema.
        
        Returns:
            PyArrow schema object
            
        Raises:
            ImportError: If PyArrow is not available
        """
        pass


class ArrowSchema(Schema):
    """
    Schema implementation based on PyArrow schema.
    """
    
    def __init__(self, arrow_schema):
        """
        Initialize ArrowSchema from PyArrow schema.
        
        Args:
            arrow_schema: PyArrow schema object
        """
        try:
            import pyarrow as pa
        except ImportError:
            raise ImportError("PyArrow is required for ArrowSchema")
            
        if not isinstance(arrow_schema, pa.Schema):
            raise ValueError("Input must be a PyArrow Schema object")
            
        self._arrow_schema = arrow_schema
        self._field_info_cache = {}
    
    def get_field_names(self) -> List[str]:
        """
        Returns a list of field names in the schema.
        
        Returns:
            List of field names
        """
        return self._arrow_schema.names
    
    def get_field_types(self) -> Dict[str, str]:
        """
        Returns a dictionary mapping field names to their types.
        
        Returns:
            Dictionary of field name to type mappings
        """
        return {name: str(field.type) for name, field in zip(self._arrow_schema.names, self._arrow_schema)}
    
    def get_primary_keys(self) -> Optional[List[str]]:
        """
        Returns the primary key field names if any.
        
        Note: PyArrow schemas don't have built-in primary key concept,
        so this returns None unless specified in metadata.
        
        Returns:
            List of primary key field names or None
        """
        if self._arrow_schema.metadata:
            primary_keys_bytes = self._arrow_schema.metadata.get(b'primary_keys')
            if primary_keys_bytes:
                # Decode and parse primary keys from metadata
                primary_keys_str = primary_keys_bytes.decode('utf-8')
                return primary_keys_str.split(',') if primary_keys_str else None
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the schema to a dictionary representation.
        
        Returns:
            Dictionary representation of the schema
        """
        fields = []
        for name, field in zip(self._arrow_schema.names, self._arrow_schema):
            field_dict = {
                'name': name,
                'type': str(field.type),
                'nullable': field.nullable,
                'metadata': dict(field.metadata) if field.metadata else {}
            }
            fields.append(field_dict)
        
        return {
            'field_names': self.get_field_names(),
            'field_count': len(self._arrow_schema),
            'fields': fields,
            'primary_key': self.get_primary_keys(),
            'metadata': dict(self._arrow_schema.metadata) if self._arrow_schema.metadata else {}
        }
    
    def get_field_info(self, field_name: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific field.
        
        Args:
            field_name: Name of the field to get info for
            
        Returns:
            Dictionary containing field information
        """
        if field_name not in self._field_info_cache:
            try:
                field_index = self._arrow_schema.names.index(field_name)
                field = self._arrow_schema.field(field_index)
                
                field_info = {
                    'name': field.name,
                    'type': str(field.type),
                    'nullable': field.nullable,
                    'metadata': dict(field.metadata) if field.metadata else {},
                    'index': field_index
                }
                
                # Add type-specific information safely
                try:
                    if hasattr(field.type, 'precision'):
                        field_info['precision'] = field.type.precision
                except (AttributeError, ValueError):
                    pass
                    
                try:
                    if hasattr(field.type, 'scale'):
                        field_info['scale'] = field.type.scale
                except (AttributeError, ValueError):
                    pass
                    
                try:
                    if hasattr(field.type, 'byte_width'):
                        field_info['byte_width'] = field.type.byte_width
                except (AttributeError, ValueError):
                    pass
                    
                self._field_info_cache[field_name] = field_info
                
            except ValueError:
                raise ValueError(f"Field '{field_name}' not found in schema")
                
        return self._field_info_cache[field_name]
    
    def validate(self) -> Dict[str, Any]:
        """
        Validate the schema and return validation results.
        
        Returns:
            Dictionary containing validation results and warnings
        """
        validation_result = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        # Check for duplicate field names
        field_names = self.get_field_names()
        if len(field_names) != len(set(field_names)):
            validation_result['valid'] = False
            validation_result['errors'].append("Schema contains duplicate field names")
        
        # Check for empty field names
        if any(not name or not name.strip() for name in field_names):
            validation_result['valid'] = False
            validation_result['errors'].append("Schema contains empty field names")
        
        # Warn about complex types that might not be supported
        complex_types = []
        for name, field in zip(self._arrow_schema.names, self._arrow_schema):
            if str(field.type).startswith(('list<', 'struct<', 'map<')):
                complex_types.append(f"{name}: {field.type}")
        
        if complex_types:
            validation_result['warnings'].append(
                f"Complex types found that might need special handling: {', '.join(complex_types)}"
            )
        
        return validation_result
    
    def to_arrow_schema(self):
        """
        Convert this schema to a PyArrow schema.
        
        Returns:
            PyArrow schema object
        """
        return self._arrow_schema
    
    def with_primary_keys(self, primary_keys: List[str]) -> 'ArrowSchema':
        """
        Create a new ArrowSchema with primary keys metadata.
        
        Args:
            primary_keys: List of field names that form the primary key
            
        Returns:
            New ArrowSchema instance with primary key metadata
        """
        # Validate that all primary key fields exist
        field_names = self.get_field_names()
        for pk in primary_keys:
            if pk not in field_names:
                raise ValueError(f"Primary key field '{pk}' not found in schema")
        
        # Create new metadata with primary keys
        existing_metadata = dict(self._arrow_schema.metadata) if self._arrow_schema.metadata else {}
        existing_metadata[b'primary_keys'] = ','.join(primary_keys).encode('utf-8')
        
        # Create new schema with updated metadata
        new_schema = self._arrow_schema.with_metadata(existing_metadata)
        return ArrowSchema(new_schema)
    
    def select(self, field_names: List[str]) -> 'ArrowSchema':
        """
        Create a new ArrowSchema with only the specified fields.
        
        Args:
            field_names: List of field names to include
            
        Returns:
            New ArrowSchema instance with selected fields
        """
        # Validate that all fields exist
        schema_field_names = self.get_field_names()
        for field_name in field_names:
            if field_name not in schema_field_names:
                raise ValueError(f"Field '{field_name}' not found in schema")
        
        # Select fields by creating a new schema with only the specified fields
        import pyarrow as pa
        selected_fields = []
        for field_name in field_names:
            field_index = self._arrow_schema.names.index(field_name)
            selected_fields.append(self._arrow_schema.field(field_index))
        
        # Create new schema with selected fields
        selected_schema = pa.schema(selected_fields, metadata=self._arrow_schema.metadata)
        return ArrowSchema(selected_schema)
    
    def __str__(self) -> str:
        """String representation of the schema."""
        return f"ArrowSchema({self._arrow_schema})"
    
    def __repr__(self) -> str:
        """Detailed string representation of the schema."""
        return f"ArrowSchema(fields={len(self._arrow_schema)}, names={self.get_field_names()})"
    
    def __eq__(self, other) -> bool:
        """Check equality with another schema."""
        if not isinstance(other, ArrowSchema):
            return False
        return self._arrow_schema.equals(other._arrow_schema)


def create_schema_from_dict(schema_dict: Dict[str, Any]) -> Schema:
    """
    Create a Schema from a dictionary representation.
    
    Args:
        schema_dict: Dictionary containing schema information
        
    Returns:
        Schema instance
    """
    try:
        import pyarrow as pa
    except ImportError:
        raise ImportError("PyArrow is required to create schema from dictionary")
    
    if 'fields' not in schema_dict:
        raise ValueError("Schema dictionary must contain 'fields' key")
    
    fields = []
    for field_info in schema_dict['fields']:
        field_name = field_info['name']
        field_type_str = field_info['type']
        nullable = field_info.get('nullable', True)
        metadata = field_info.get('metadata', {})
        
        # Convert string type to PyArrow type
        try:
            field_type = _string_to_arrow_type(field_type_str)
        except Exception as e:
            raise ValueError(f"Cannot convert type '{field_type_str}' for field '{field_name}': {e}")
        
        field = pa.field(field_name, field_type, nullable=nullable, metadata=metadata)
        fields.append(field)
    
    # Create schema with metadata
    schema_metadata = schema_dict.get('metadata', {})
    if 'primary_key' in schema_dict and schema_dict['primary_key']:
        schema_metadata[b'primary_keys'] = ','.join(schema_dict['primary_key']).encode('utf-8')
    
    arrow_schema = pa.schema(fields, metadata=schema_metadata if schema_metadata else None)
    return ArrowSchema(arrow_schema)


def _string_to_arrow_type(type_str: str):
    """
    Convert string type representation to PyArrow type.
    
    Args:
        type_str: String representation of the type
        
    Returns:
        PyArrow data type
    """
    import pyarrow as pa
    
    # Basic type mappings
    type_mapping = {
        'bool': pa.bool_(),
        'int8': pa.int8(),
        'int16': pa.int16(),
        'int32': pa.int32(),
        'int64': pa.int64(),
        'uint8': pa.uint8(),
        'uint16': pa.uint16(),
        'uint32': pa.uint32(),
        'uint64': pa.uint64(),
        'float': pa.float32(),
        'float32': pa.float32(),
        'float64': pa.float64(),
        'double': pa.float64(),
        'string': pa.string(),
        'binary': pa.binary(),
        'date32': pa.date32(),
        'date64': pa.date64(),
        'timestamp': pa.timestamp('us'),
    }
    
    # Check for exact match first
    if type_str in type_mapping:
        return type_mapping[type_str]
    
    # Handle parameterized types
    if type_str.startswith('decimal'):
        # Extract precision and scale from decimal(precision, scale)
        import re
        match = re.match(r'decimal\((\d+),?\s*(\d+)?\)', type_str)
        if match:
            precision = int(match.group(1))
            scale = int(match.group(2)) if match.group(2) else 0
            return pa.decimal128(precision, scale)
    
    elif type_str.startswith('timestamp'):
        # Handle timestamp with timezone
        import re
        match = re.match(r'timestamp\[(.+)\]', type_str)
        if match:
            unit = match.group(1)
            return pa.timestamp(unit)
    
    elif type_str.startswith('list<') and type_str.endswith('>'):
        # Handle list types
        inner_type_str = type_str[5:-1]
        inner_type = _string_to_arrow_type(inner_type_str)
        return pa.list_(inner_type)
    
    # If no match found, try to parse it directly with PyArrow
    try:
        return eval(f"pa.{type_str}()")
    except:
        raise ValueError(f"Unknown type: {type_str}")
