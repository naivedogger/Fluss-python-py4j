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

from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class FlussDataWriter:
    """
    High-level data writer for Fluss tables.
    
    This class provides a Python-friendly interface for writing data to Fluss tables,
    handling type conversions and batch operations automatically.
    """
    
    def __init__(self, java_writer, gateway):
        """
        Initialize the data writer.
        
        Args:
            java_writer: The underlying Java data writer instance
            gateway: The Py4J gateway for Java type conversions
        """
        self._java_writer = java_writer
        self._gateway = gateway
        self._is_closed = False
        
    def write_row(self, data: Dict[str, Any]) -> bool:
        """
        Write a single row of data.
        
        Args:
            data: Dictionary containing column_name -> value mappings
            
        Returns:
            True if write was successful
        """
        self._check_not_closed()
        
        try:
            # Convert Python dict to Java array
            # This is a simplified version - in real usage, you'd need to know the schema
            values = list(data.values())
            java_row = self._gateway.new_array(self._gateway.jvm.java.lang.Object, len(values))
            
            for i, value in enumerate(values):
                if isinstance(value, int):
                    java_row[i] = self._gateway.jvm.java.lang.Long(value)
                elif isinstance(value, float):
                    java_row[i] = self._gateway.jvm.java.lang.Double(value)
                else:
                    java_row[i] = str(value)
            
            # Create a single-item list and write it
            data_list = self._gateway.jvm.java.util.ArrayList()
            data_list.add(java_row)
            
            write_count = self._java_writer.writeDataWithUpsert(data_list)
            return write_count > 0
            
        except Exception as e:
            logger.error(f"Error writing row: {e}")
            return False
            
    def write_rows(self, data_list: List[Dict[str, Any]]) -> int:
        """
        Write multiple rows of data.
        
        Args:
            data_list: List of dictionaries, each containing column_name -> value mappings
            
        Returns:
            Number of successfully written rows
        """
        self._check_not_closed()
        
        try:
            # Convert all rows to Java format
            java_data_list = self._gateway.jvm.java.util.ArrayList()
            
            for row_data in data_list:
                values = list(row_data.values())
                java_row = self._gateway.new_array(self._gateway.jvm.java.lang.Object, len(values))
                
                for i, value in enumerate(values):
                    if isinstance(value, int):
                        java_row[i] = self._gateway.jvm.java.lang.Long(value)
                    elif isinstance(value, float):
                        java_row[i] = self._gateway.jvm.java.lang.Double(value)
                    else:
                        java_row[i] = str(value)
                
                java_data_list.add(java_row)
            
            # Write all rows in one batch
            write_count = self._java_writer.writeDataWithUpsert(java_data_list)
            logger.info(f"Successfully wrote {write_count}/{len(data_list)} rows")
            return write_count
            
        except Exception as e:
            logger.error(f"Error writing rows: {e}")
            return 0
        
    def flush(self):
        """Force flush any buffered data."""
        self._check_not_closed()
        # The Java writer doesn't have flush method, so this is a no-op
        logger.debug("Flush called (no-op for Java writer)")
            
    def close(self):
        """Close the writer and release resources."""
        if not self._is_closed:
            try:
                # The Java writer doesn't have close method, so just mark as closed
                self._is_closed = True
                logger.debug("Data writer closed")
            except Exception as e:
                logger.error(f"Error closing writer: {e}")
                
    def __enter__(self):
        """Context manager entry."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        
    def _check_not_closed(self):
        """Check if writer is still open."""
        if self._is_closed:
            raise RuntimeError("Writer has been closed")
            
    def __del__(self):
        """Destructor to ensure cleanup."""
        self.close()
