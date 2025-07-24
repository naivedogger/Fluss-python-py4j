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
import logging
import atexit
import subprocess
import time
from typing import Optional, Dict, Any, List, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from pyfluss.api.table import Table
    from pyfluss.api.schema import Schema

logger = logging.getLogger(__name__)

class FlussConnection:
    """
    PyFluss connection manager that automatically handles Java gateway lifecycle.
    
    This class provides a high-level interface for connecting to Fluss clusters,
    managing the underlying Java gateway connection transparently.
    """
    
    def __init__(self, server_address: str = "localhost:9123"):
        """
        Initialize a Fluss connection.
        
        Args:
            server_address: Fluss server address in format "host:port"
        """
        self.server_address = server_address
        self._gateway = None
        self._java_process = None     # Process for Java gateway server
        self._java_app = None         # Main Java application entry point
        self._java_connection = None  # 缓存Java连接对象，这是主要的API入口
        self._is_connected = False
        
    def __enter__(self):
        """Context manager entry - establish connection."""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close connection."""
        self.close()
        
    def connect(self) -> 'FlussConnection':
        """
        Establish connection to Fluss cluster.
        
        Returns:
            Self for method chaining
        """
        if self._is_connected:
            logger.warning("Connection already established")
            return self
            
        try:
            # Get JAR path
            jar_path = self._get_jar_path()
            
            # Start Java gateway server process
            self._start_java_gateway_server(jar_path)
            
            # Wait a moment for server to start
            time.sleep(2)
            
            # Create Java gateway client
            import py4j.java_gateway
            self._gateway = py4j.java_gateway.JavaGateway()
            
            # Get the main Java application entry point
            self._java_app = self._gateway.entry_point
            
            # 创建并缓存Java连接对象
            self._java_connection = self._java_app.createConnection(self.server_address)
            
            # Test connection
            self._test_connection()
            
            self._is_connected = True
            logger.info(f"Successfully connected to Fluss at {self.server_address}")
            
            # Register cleanup on exit
            atexit.register(self.close)
            
        except Exception as e:
            logger.error(f"Failed to connect to Fluss: {e}")
            self.close()
            raise ConnectionError(f"Could not establish connection to Fluss: {e}")
            
        return self
        
    def close(self):
        """Close the connection and cleanup resources."""
        if not self._is_connected:
            return
            
        try:
            # 清理Java连接对象
            if self._java_connection:
                try:
                    self._java_connection.close()
                except:
                    pass
                self._java_connection = None
                
            # Clean up gateway and Java process
            if self._gateway:
                self._gateway.shutdown()
                self._gateway = None
                
            if self._java_process:
                self._java_process.terminate()
                self._java_process.wait(timeout=5)  # Wait up to 5 seconds
                self._java_process = None
                
            self._java_app = None
            self._is_connected = False
            logger.info("Fluss connection closed")
            
        except Exception as e:
            logger.warning(f"Error during connection cleanup: {e}")
            
    def is_connected(self) -> bool:
        """Check if connection is active."""
        return self._is_connected and self._gateway is not None
        
    def get_admin(self, name: str = "default"):
        """
        Get a catalog instance.
        
        Args:
            name: Catalog name
            
        Returns:
            Admin instance for catalog operations
        """
        connection = self._get_connection()
        return connection.getAdmin()  # Return admin for catalog operations
        
    def create_writer(self, table_path: str):
        """
        Create a data writer for the specified table.
        
        Args:
            table_path: Full table path (e.g., 'database.table' or 'catalog.database.table')
            
        Returns:
            Data writer instance
        """
        self._ensure_connected()
        
        # Parse table path
        parts = table_path.split('.')
        if len(parts) == 2:
            database_name, table_name = parts
        elif len(parts) == 3:
            # catalog.database.table format
            _, database_name, table_name = parts
        else:
            raise ValueError(f"Invalid table path format: {table_path}. Expected 'database.table' or 'catalog.database.table'")
        
        # 使用缓存的连接
        connection = self._get_connection()
        table = connection.getTable(database_name, table_name)
        java_writer = self._java_app.createDataWriter(table)
        from .writer import FlussDataWriter
        return FlussDataWriter(java_writer, self._gateway)
        
    def create_reader(self, table_path: str, **kwargs):
        """
        Create a data reader for the specified table.
        
        Args:
            table_path: Full table path (e.g., 'database.table' or 'catalog.database.table')
            **kwargs: Additional reader options
            
        Returns:
            Data reader instance
        """
        self._ensure_connected()
        
        # Parse table path
        parts = table_path.split('.')
        if len(parts) == 2:
            database_name, table_name = parts
        elif len(parts) == 3:
            # catalog.database.table format
            _, database_name, table_name = parts
        else:
            raise ValueError(f"Invalid table path format: {table_path}. Expected 'database.table' or 'catalog.database.table'")
        
        # 使用缓存的连接
        connection = self._get_connection()
        table = connection.getTable(database_name, table_name)
        
        reader_factory = self._gateway.jvm.org.example.FlussDataReaderFactory()
        java_reader = reader_factory.createScanReader(table, "snapshot")
            
        from .reader import FlussDataReader
        return FlussDataReader(java_reader, self._gateway)
        
    def execute_sql(self, sql: str):
        """
        Execute SQL query (if supported in future versions).
        
        Args:
            sql: SQL query string
            
        Returns:
            Query result
        """
        self._ensure_connected()
        # Placeholder for future SQL support
        raise NotImplementedError("SQL execution not yet implemented")
        
    def create_database(self, database_name: str, database_descriptor=None, 
                        if_not_exists: bool = True, comment: str = None, 
                        custom_properties: Dict[str, str] = None):
        """
        Create a database.
        
        Args:
            database_name: Name of the database to create
            database_descriptor: DatabaseDescriptor object (preferred) or None to use other parameters
            if_not_exists: Whether to ignore if database already exists (used when database_descriptor is None)
            comment: Database comment (used when database_descriptor is None)
            custom_properties: Custom properties (used when database_descriptor is None)
            
        Returns:
            True if successful
        """
        self._ensure_connected()
        
        try:
            # 使用缓存的连接获取admin
            connection = self._get_connection()
            admin = connection.getAdmin()
            
            # Handle DatabaseDescriptor or create one from parameters
            if database_descriptor is not None:
                # Use provided DatabaseDescriptor
                from pyfluss.api.metadata import DatabaseDescriptor
                if not isinstance(database_descriptor, DatabaseDescriptor):
                    raise ValueError("database_descriptor must be a DatabaseDescriptor instance")
                desc = database_descriptor
            else:
                # Create DatabaseDescriptor from parameters (backward compatibility)
                from pyfluss.api.metadata import DatabaseDescriptor
                builder = DatabaseDescriptor.builder()
                if comment:
                    builder = builder.comment(comment)
                if custom_properties:
                    builder = builder.custom_properties(custom_properties)
                desc = builder.build()
            
            # Create Java DatabaseDescriptor using builder pattern
            java_builder = self._gateway.jvm.com.alibaba.fluss.metadata.DatabaseDescriptor.builder()
            
            if desc.comment:
                java_builder = java_builder.comment(desc.comment)
            
            # Set custom properties if provided
            if desc.custom_properties:
                try:
                    # Create a Java HashMap for custom properties
                    java_map = self._gateway.jvm.java.util.HashMap()
                    for key, value in desc.custom_properties.items():
                        java_map.put(key, value)
                    
                    # Use the customProperties method that accepts Map<String, String>
                    java_builder = java_builder.customProperties(java_map)
                    logger.debug(f"Added custom properties: {desc.custom_properties}")
                except Exception as e:
                    logger.warning(f"Failed to set custom properties: {e}. Properties will be ignored.")
                
            java_database_descriptor = java_builder.build()

            def create_operation():
                return admin.createDatabase(database_name, java_database_descriptor, if_not_exists)
            
            # 直接变成同步。也可以考虑返回 CompletableFuture，然后再暴露 handle_async 这个接口
            self._handle_async_operation_with_retry(create_operation)
            
            logger.info(f"Database created successfully: {database_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create database {database_name}: {e}")
            return False
            
    def create_table(self, database_name: str, table_name: str, 
                     schema_or_columns: Union['Schema', List[Dict[str, str]]], 
                     primary_keys: List[str] = None, if_not_exists: bool = True):
        """
        Create a table with the specified schema.
        
        Args:
            database_name: Database name
            table_name: Table name
            schema_or_columns: Either a Schema object or list of column definitions (each dict should have 'name' and 'type')
            primary_keys: List of primary key column names (only used if schema_or_columns is a list)
            if_not_exists: Whether to ignore if table already exists
            
        Returns:
            True if successful
        """
        self._ensure_connected()
        
        try:
            # 使用缓存的连接获取admin
            connection = self._get_connection()
            admin = connection.getAdmin()
            
            # Handle different input types
            from pyfluss.api.schema import Schema
            if isinstance(schema_or_columns, Schema):
                # Use Schema object
                schema_columns = []
                field_types = schema_or_columns.get_field_types()
                for field_name in schema_or_columns.get_field_names():
                    schema_columns.append({
                        'name': field_name,
                        'type': field_types[field_name]
                    })
                primary_keys = schema_or_columns.get_primary_keys()
            else:
                # Use list of column definitions
                schema_columns = schema_or_columns
            
            # Build schema
            schema_builder = self._gateway.jvm.com.alibaba.fluss.metadata.Schema.newBuilder()
            
            # Add columns
            for col in schema_columns:
                col_name = col['name']
                col_type = col['type'].upper()
                
                # Map Python type names to Fluss DataTypes
                data_type = self._map_type_to_fluss_datatype(col_type)
                schema_builder.column(col_name, data_type)
            
            # Set primary keys if specified
            if primary_keys:
                pk_array = self._gateway.new_array(self._gateway.jvm.java.lang.String, len(primary_keys))
                for i, pk in enumerate(primary_keys):
                    pk_array[i] = pk
                schema_builder.primaryKey(pk_array)
            
            schema = schema_builder.build()
            
            # Create table descriptor
            table_descriptor = self._gateway.jvm.com.alibaba.fluss.metadata.TableDescriptor.builder().schema(schema).build()
            
            # Create table
            table_path = self._gateway.jvm.com.alibaba.fluss.metadata.TablePath.of(database_name, table_name)
            
            def create_operation():
                return admin.createTable(table_path, table_descriptor, if_not_exists)
            
            self._handle_async_operation_with_retry(create_operation)
            
            logger.info(f"Table created successfully: {database_name}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create table {database_name}.{table_name}: {e}")
            return False
            
    def list_databases(self):
        """
        List all databases.
        
        Returns:
            List of database names
        """
        self._ensure_connected()
        
        try:
            # 使用缓存的连接获取admin
            connection = self._get_connection()
            admin = connection.getAdmin()
            
            def list_operation():
                return admin.listDatabases()
            
            databases = self._handle_async_operation_with_retry(list_operation)
            # Convert Java collection to Python list - use iterator approach
            result = []
            try:
                iterator = databases.iterator()
                while iterator.hasNext():
                    result.append(str(iterator.next()))
            except:
                # Fallback: assume it's already a list-like
                try:
                    result = [str(db) for db in databases]
                except:
                    logger.warning("Could not iterate over databases")
                    result = []
            return result
        except Exception as e:
            logger.error(f"Failed to list databases: {e}")
            return []
            
    def list_tables(self, database_name: str):
        """
        List all tables in a database.
        
        Args:
            database_name: Database name
            
        Returns:
            List of table names
        """
        self._ensure_connected()
        
        try:
            # 使用缓存的连接获取admin
            connection = self._get_connection()
            admin = connection.getAdmin()
            
            def list_operation():
                return admin.listTables(database_name)
            
            tables = self._handle_async_operation_with_retry(list_operation)
            # Convert Java collection to Python list - use iterator approach
            result = []
            try:
                iterator = tables.iterator()
                while iterator.hasNext():
                    result.append(str(iterator.next()))
            except:
                # Fallback: assume it's already a list-like
                try:
                    result = [str(table) for table in tables]
                except:
                    logger.warning(f"Could not iterate over tables in database {database_name}")
                    result = []
            return result
        except Exception as e:
            logger.error(f"Failed to list tables in database {database_name}: {e}")
            return []

    def _handle_async_operation(self, future_result, timeout: int = 30):
        """
        Handle Java CompletableFuture operations synchronously.
        
        Args:
            future_result: Java CompletableFuture object
            timeout: Timeout in seconds
            
        Returns:
            The result of the async operation
            
        Raises:
            TimeoutError: If operation times out
            RuntimeError: If operation fails
        """
        try:
            # Check if this is already a completed result (not a future)
            if not hasattr(future_result, 'get'):
                return future_result
                
            # Use Java's get() method with timeout
            import py4j.java_gateway
            TimeUnit = self._gateway.jvm.java.util.concurrent.TimeUnit
            result = future_result.get(timeout, TimeUnit.SECONDS)
            return result
            
        except py4j.java_gateway.Py4JJavaError as e:
            if "TimeoutException" in str(e):
                raise TimeoutError(f"Operation timed out after {timeout} seconds")
            else:
                raise RuntimeError(f"Async operation failed: {e}")
        except Exception as e:
            raise RuntimeError(f"Failed to handle async operation: {e}")

    def _handle_async_operation_with_retry(self, operation_func, max_retries: int = 3, timeout: int = 30):
        """
        Handle async operations with retry logic.
        
        Args:
            operation_func: Function that returns a CompletableFuture
            max_retries: Maximum number of retry attempts
            timeout: Timeout per attempt in seconds
            
        Returns:
            The result of the async operation
        """
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                future_result = operation_func()
                return self._handle_async_operation(future_result, timeout)
            except Exception as e:
                last_exception = e
                logger.warning(f"Async operation attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(1 * (attempt + 1))  # Exponential backoff
                    
        raise RuntimeError(f"Async operation failed after {max_retries} attempts. Last error: {last_exception}")

    def drop_database(self, database_name: str, if_exists: bool = True):
        """
        Drop a database.
        
        Args:
            database_name: Name of the database to drop
            if_exists: Whether to ignore if database doesn't exist
            
        Returns:
            True if successful
        """
        self._ensure_connected()
        
        try:
            # 使用缓存的连接获取admin
            connection = self._get_connection()
            admin = connection.getAdmin()
            
            def drop_operation():
                return admin.dropDatabase(database_name, False, False)  # cascade=False, ignoreIfNotExists=False
                
            self._handle_async_operation_with_retry(drop_operation)
            logger.info(f"Database dropped successfully: {database_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to drop database {database_name}: {e}")
            return False

    def drop_table(self, database_name: str, table_name: str, if_exists: bool = True):
        """
        Drop a table.
        
        Args:
            database_name: Database name
            table_name: Table name
            if_exists: Whether to ignore if table doesn't exist
            
        Returns:
            True if successful
        """
        self._ensure_connected()
        
        try:
            # 使用缓存的连接获取admin
            connection = self._get_connection()
            admin = connection.getAdmin()
            
            # Create table path
            table_path = self._gateway.jvm.com.alibaba.fluss.metadata.TablePath.of(database_name, table_name)
            
            def drop_operation():
                return admin.dropTable(table_path, False)  # ignoreIfNotExists = False
                
            self._handle_async_operation_with_retry(drop_operation)
            logger.info(f"Table dropped successfully: {database_name}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to drop table {database_name}.{table_name}: {e}")
            return False

    def get_table_schema(self, database_name: str, table_name: str):
        """
        Get table schema information.
        
        Args:
            database_name: Database name
            table_name: Table name
            
        Returns:
            Table schema information or None if not found
        """
        self._ensure_connected()
        
        try:
            # 使用缓存的连接
            connection = self._get_connection()
            table = connection.getTable(database_name, table_name)
            
            # Get table info and schema
            table_info = table.getTableInfo()
            row_type = table_info.getRowType()
            
            # Get column information
            field_names = row_type.getFieldNames()
            field_types = row_type.getChildren()  # getChildren() returns List<DataType>
            
            columns = []
            for i in range(field_names.size()):
                columns.append({
                    'name': field_names.get(i),
                    'type': str(field_types.get(i)),
                    'nullable': True  # Default assumption
                })
            
            # Get primary keys (if available)
            primary_keys = []
            # Primary key extraction from table schema if available
            
            return {
                'columns': columns,
                'primary_keys': primary_keys,
                'table_path': f"{database_name}.{table_name}"
            }
            
        except Exception as e:
            logger.error(f"Failed to get table schema for {database_name}.{table_name}: {e}")
            return None

    def _map_type_to_fluss_datatype(self, col_type: str):
        """
        Map Python type string to Fluss DataType.
        
        Args:
            col_type: Type string (e.g., 'STRING', 'INT64', etc.)
            
        Returns:
            Fluss DataType object
        """
        col_type = col_type.upper()
        
        # Handle PyArrow types
        if col_type in ['STRING', 'VARCHAR']:
            return self._gateway.jvm.com.alibaba.fluss.types.DataTypes.STRING()
        elif col_type in ['INT64', 'BIGINT', 'LONG']:
            return self._gateway.jvm.com.alibaba.fluss.types.DataTypes.BIGINT()
        elif col_type in ['INT32', 'INT', 'INTEGER']:
            return self._gateway.jvm.com.alibaba.fluss.types.DataTypes.INT()
        elif col_type in ['FLOAT64', 'DOUBLE', 'FLOAT']:
            return self._gateway.jvm.com.alibaba.fluss.types.DataTypes.DOUBLE()
        elif col_type in ['FLOAT32']:
            return self._gateway.jvm.com.alibaba.fluss.types.DataTypes.FLOAT()
        elif col_type in ['BOOL', 'BOOLEAN']:
            return self._gateway.jvm.com.alibaba.fluss.types.DataTypes.BOOLEAN()
        elif col_type.startswith('TIMESTAMP'):
            # Handle timestamp types
            return self._gateway.jvm.com.alibaba.fluss.types.DataTypes.TIMESTAMP(3)  # Default precision
        elif col_type.startswith('DECIMAL'):
            # Handle decimal types - extract precision and scale if available
            import re
            match = re.match(r'DECIMAL\((\d+),?\s*(\d+)?\)', col_type)
            if match:
                precision = int(match.group(1))
                scale = int(match.group(2)) if match.group(2) else 0
                return self._gateway.jvm.com.alibaba.fluss.types.DataTypes.DECIMAL(precision, scale)
            else:
                return self._gateway.jvm.com.alibaba.fluss.types.DataTypes.DECIMAL(10, 2)  # Default
        else:
            # Default to string for unknown types
            logger.warning(f"Unknown type '{col_type}', defaulting to STRING")
            return self._gateway.jvm.com.alibaba.fluss.types.DataTypes.STRING()

    def _ensure_connected(self):
        """Ensure connection is established."""
        if not self.is_connected():
            raise ConnectionError("Not connected to Fluss. Call connect() first.")
            
    def _get_jar_path(self) -> str:
        """Get the path to the Fluss JAR file."""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        jar_path = os.path.join(current_dir, "jars", "fluss-python-py4j-1.0-SNAPSHOT.jar")
        
        if not os.path.exists(jar_path):
            raise FileNotFoundError(f"Fluss JAR not found at {jar_path}")
            
        return jar_path
        
    def _start_java_gateway_server(self, jar_path: str):
        """Start the Java gateway server process."""
        # Clean up any existing Java processes
        try:
            subprocess.run(["pkill", "-f", "SimplePy4JGatewayServer"], check=False)
            time.sleep(1)  # Give processes time to clean up
        except Exception:
            pass  # Ignore errors in cleanup
        
        # Start new Java process
        java_cmd = [
            "java",
            "-cp", jar_path,
            "org.example.SimplePy4JGatewayServer"
        ]
        
        logger.debug(f"Starting Java gateway server: {' '.join(java_cmd)}")
        
        self._java_process = subprocess.Popen(
            java_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Check if process started successfully
        time.sleep(1)
        if self._java_process.poll() is not None:
            stdout, stderr = self._java_process.communicate()
            error_msg = f"Java process failed to start. stdout: {stdout.decode()}, stderr: {stderr.decode()}"
            raise RuntimeError(error_msg)
            
    def _test_connection(self):
        """Test if the Java gateway connection is working."""
        try:
            # Try to call a simple method to verify connection
            result = self._java_app.toString()
            logger.debug(f"Connection test result: {result}")
        except Exception as e:
            raise ConnectionError(f"Gateway connection test failed: {e}")
    
    def _get_connection(self):
        """获取缓存的Connection对象，这是主要的API入口"""
        self._ensure_connected()
        
        if self._java_connection is None:
            self._java_connection = self._java_app.createConnection(self.server_address)
            
        return self._java_connection

    def getAdmin(self):
        """
        Get admin instance for administrative operations.
        
        Returns:
            Admin wrapper instance
        """
        connection = self._get_connection()
        java_admin = connection.getAdmin()
        from pyfluss.api.admin import Admin
        return Admin(java_admin, self._gateway, self)
        

# Global connection instance for convenience
_global_connection: Optional[FlussConnection] = None

def connect(server_address: str = "localhost:9123") -> FlussConnection:
    """
    Create and establish a connection to Fluss cluster.
    
    This is the main entry point for PyFluss. It automatically manages
    the Java gateway connection and provides a clean API.
    
    Args:
        server_address: Fluss server address in format "host:port"
        
    Returns:
        Connected FlussConnection instance
        
    Example:
        >>> import pyfluss
        >>> conn = pyfluss.connect("localhost:9123")
        >>> catalog = conn.get_catalog()
        >>> writer = conn.create_writer("my_database.my_table")
    """
    global _global_connection
    
    # Close existing global connection if any
    if _global_connection:
        _global_connection.close()
        
    _global_connection = FlussConnection(server_address)
    _global_connection.connect()
    return _global_connection

def disconnect():
    """Close the global connection."""
    global _global_connection
    if _global_connection:
        _global_connection.close()
        _global_connection = None

def get_connection() -> Optional[FlussConnection]:
    """Get the current global connection."""
    return _global_connection
