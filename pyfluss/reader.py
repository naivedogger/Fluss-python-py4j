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

from typing import List, Dict, Any, Optional, Iterator
import logging

logger = logging.getLogger(__name__)

class FlussDataReader:
    """
    High-level data reader for Fluss tables.
    
    This class provides a Python-friendly interface for reading data from Fluss tables,
    with support for filtering, iteration, and batch reading.
    """
    
    def __init__(self, java_reader, gateway):
        """
        Initialize the data reader.
        
        Args:
            java_reader: The underlying Java data reader instance
            gateway: The Py4J gateway for Java type conversions
        """
        self._java_reader = java_reader
        self._gateway = gateway
        self._is_closed = False
        
    def read_row(self) -> Optional[Dict[str, Any]]:
        """
        Read a single row of data.
        
        Returns:
            Dictionary containing column_name -> value mappings, or None if no more data
        """
        self._check_not_closed()
        
        try:
            # Use the Java readBatchData method to get one row
            batch_data = self._java_reader.readBatchData(1)
            if batch_data and len(batch_data) > 0:
                # Convert Java result to Python dict
                return self._convert_java_result(batch_data[0])
            return None
            
        except Exception as e:
            logger.error(f"Error reading row: {e}")
            return None
            
    def read_rows(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Read multiple rows of data.
        
        Args:
            limit: Maximum number of rows to read
            
        Returns:
            List of dictionaries containing the row data
        """
        self._check_not_closed()
        
        try:
            # Use the Java readBatchData method directly
            batch_data = self._java_reader.readBatchData(limit)
            rows = []
            if batch_data:
                for java_row in batch_data:
                    row = self._convert_java_result(java_row)
                    if row:
                        rows.append(row)
            
            logger.debug(f"Read {len(rows)} rows")
            return rows
            
        except Exception as e:
            logger.error(f"Error reading rows: {e}")
            return []
        
    def read_all(self) -> List[Dict[str, Any]]:
        """
        Read all available data.
        
        Warning: This may consume large amounts of memory for large datasets.
        
        Returns:
            List of all available rows
        """
        self._check_not_closed()
        
        rows = []
        while True:
            row = self.read_row()
            if row is None:
                break
            rows.append(row)
            
        logger.info(f"Read all {len(rows)} rows")
        return rows
        
    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """Make the reader iterable."""
        return self
        
    def __next__(self) -> Dict[str, Any]:
        """Iterator protocol implementation."""
        row = self.read_row()
        if row is None:
            raise StopIteration
        return row
        
    def close(self):
        """Close the reader and release resources."""
        if not self._is_closed:
            try:
                # The Java reader doesn't have close method, so just mark as closed
                self._is_closed = True
                logger.debug("Data reader closed")
            except Exception as e:
                logger.error(f"Error closing reader: {e}")
                
    def __enter__(self):
        """Context manager entry."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        
    def _check_not_closed(self):
        """Check if reader is still open."""
        if self._is_closed:
            raise RuntimeError("Reader has been closed")
            
    def _convert_java_result(self, java_result) -> Dict[str, Any]:
        """
        Convert Java result object to Python dictionary.
        
        Args:
            java_result: Result from Java reader
            
        Returns:
            Python dictionary representation
        """
        # This is a placeholder implementation
        # The actual conversion depends on how the Java side structures the data
        
        if hasattr(java_result, 'toDict'):
            # If Java object has a toDict method
            return java_result.toDict()
        elif hasattr(java_result, 'toString'):
            # If it's a simple string representation
            # This is a fallback - you might need more sophisticated parsing
            result_str = java_result.toString()
            logger.debug(f"Converting Java result string: {result_str}")
            
            # Try to parse as key-value pairs (this is a simple example)
            try:
                # Assuming format like: {key1=value1, key2=value2}
                if result_str.startswith('{') and result_str.endswith('}'):
                    content = result_str[1:-1]  # Remove braces
                    pairs = content.split(', ')
                    result = {}
                    for pair in pairs:
                        if '=' in pair:
                            key, value = pair.split('=', 1)
                            result[key.strip()] = self._parse_value(value.strip())
                    return result
            except Exception as e:
                logger.warning(f"Failed to parse Java result string: {e}")
                
        # Fallback: return as-is
        # logger.warning("Using fallback conversion for Java result")
        return {"raw_result": str(java_result)}
        
    def _parse_value(self, value_str: str) -> Any:
        """
        Parse a string value to appropriate Python type.
        
        Args:
            value_str: String representation of the value
            
        Returns:
            Parsed value
        """
        # Try to convert to appropriate types
        if value_str.lower() == 'null':
            return None
        elif value_str.lower() in ('true', 'false'):
            return value_str.lower() == 'true'
        elif value_str.isdigit() or (value_str.startswith('-') and value_str[1:].isdigit()):
            return int(value_str)
        else:
            try:
                return float(value_str)
            except ValueError:
                return value_str  # Keep as string
                
    def to_fluss_table_read(self):
        """
        Convert to FlussTableRead interface for advanced operations.
        
        Returns:
            FlussTableReadImpl instance
        """
        try:
            from .api.fluss_table_read import FlussTableReadImpl
            return FlussTableReadImpl(self)
        except ImportError as e:
            logger.warning(f"Could not import FlussTableRead: {e}")
            return None
    
    def to_pandas(self, limit: Optional[int] = None):
        """
        Convert data to Pandas DataFrame.
        
        Args:
            limit: Maximum number of rows to read
            
        Returns:
            Pandas DataFrame
        """
        try:
            import pandas as pd
            rows = self.read_rows(limit or 1000)
            return pd.DataFrame(rows)
        except ImportError:
            raise ImportError("pandas is required. Install with: pip install pandas")
    
    def to_arrow(self, limit: Optional[int] = None):
        """
        Convert data to PyArrow Table.
        
        Args:
            limit: Maximum number of rows to read
            
        Returns:
            PyArrow Table
        """
        try:
            import pyarrow as pa
            df = self.to_pandas(limit)
            return pa.Table.from_pandas(df)
        except ImportError:
            raise ImportError("pyarrow is required. Install with: pip install pyarrow")

    def __del__(self):
        """Destructor to ensure cleanup."""
        self.close()
