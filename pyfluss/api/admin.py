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

from typing import Optional, Dict
from .metadata import DatabaseDescriptor


class Admin:
    """
    Admin interface for Fluss database and table operations.
    This wraps the Java admin object and provides a Python-friendly API.
    """
    
    def __init__(self, java_admin, gateway, connection_manager):
        """
        Initialize Admin with Java admin object.
        
        Args:
            java_admin: Java admin object
            gateway: Py4J gateway
            connection_manager: Connection manager for async operations
        """
        self._java_admin = java_admin
        self._gateway = gateway
        self._connection_manager = connection_manager
    
    def create_database(self, database_name: str, database_descriptor: Optional[DatabaseDescriptor] = None,
                       if_not_exists: bool = True):
        """
        Create a database.
        
        Args:
            database_name: Name of the database to create
            database_descriptor: Optional DatabaseDescriptor object
            if_not_exists: Whether to ignore if database already exists
            
        Returns:
            True if successful
        """
        # If no descriptor provided, create a default one
        if database_descriptor is None:
            database_descriptor = DatabaseDescriptor()
        
        # Create Java DatabaseDescriptor
        java_builder = self._gateway.jvm.com.alibaba.fluss.metadata.DatabaseDescriptor.builder()
        
        if database_descriptor.comment:
            java_builder = java_builder.comment(database_descriptor.comment)
        
        if database_descriptor.custom_properties:
            java_map = self._gateway.jvm.java.util.HashMap()
            for key, value in database_descriptor.custom_properties.items():
                java_map.put(key, value)
            java_builder = java_builder.customProperties(java_map)
        
        java_database_descriptor = java_builder.build()
        
        def create_operation():
            return self._java_admin.createDatabase(database_name, java_database_descriptor, if_not_exists)
        
        self._connection_manager._handle_async_operation_with_retry(create_operation)
        return True
    
    def list_databases(self):
        """
        List all databases.
        
        Returns:
            List of database names
        """
        def list_operation():
            return self._java_admin.listDatabases()
        
        databases = self._connection_manager._handle_async_operation_with_retry(list_operation)
        
        # Convert Java collection to Python list
        result = []
        try:
            iterator = databases.iterator()
            while iterator.hasNext():
                result.append(str(iterator.next()))
        except:
            try:
                result = [str(db) for db in databases]
            except:
                result = []
        return result
    
    def drop_database(self, database_name: str, cascade: bool = False, if_exists: bool = True):
        """
        Drop a database.
        
        Args:
            database_name: Name of the database to drop
            cascade: Whether to drop all tables in the database
            if_exists: Whether to ignore if database doesn't exist
            
        Returns:
            True if successful
        """
        def drop_operation():
            return self._java_admin.dropDatabase(database_name, cascade, if_exists)
        
        self._connection_manager._handle_async_operation_with_retry(drop_operation)
        return True
    
    def list_tables(self, database_name: str):
        """
        List all tables in a database.
        
        Args:
            database_name: Database name
            
        Returns:
            List of table names
        """
        def list_operation():
            return self._java_admin.listTables(database_name)
        
        tables = self._connection_manager._handle_async_operation_with_retry(list_operation)
        
        # Convert Java collection to Python list
        result = []
        try:
            iterator = tables.iterator()
            while iterator.hasNext():
                result.append(str(iterator.next()))
        except:
            try:
                result = [str(table) for table in tables]
            except:
                result = []
        return result
    
    def create_table(self, table_path, table_descriptor, if_not_exists: bool = True):
        """
        Create a table.
        
        Args:
            table_path: TablePath object or string
            table_descriptor: Table descriptor
            if_not_exists: Whether to ignore if table already exists
            
        Returns:
            True if successful
        """
        # Convert string table path to Java TablePath if needed
        if isinstance(table_path, str):
            parts = table_path.split('.')
            if len(parts) == 2:
                database_name, table_name = parts
                java_table_path = self._gateway.jvm.com.alibaba.fluss.metadata.TablePath.of(database_name, table_name)
            else:
                raise ValueError("Table path must be in format 'database.table'")
        else:
            # Assume it's already a Java TablePath or our TablePath
            if hasattr(table_path, 'to_java'):
                java_table_path = table_path.to_java(self._gateway)
            else:
                java_table_path = table_path
        
        def create_operation():
            return self._java_admin.createTable(java_table_path, table_descriptor, if_not_exists)
        
        self._connection_manager._handle_async_operation_with_retry(create_operation)
        return True
    
    def drop_table(self, table_path, if_exists: bool = True):
        """
        Drop a table.
        
        Args:
            table_path: TablePath object or string
            if_exists: Whether to ignore if table doesn't exist
            
        Returns:
            True if successful
        """
        # Convert string table path to Java TablePath if needed
        if isinstance(table_path, str):
            parts = table_path.split('.')
            if len(parts) == 2:
                database_name, table_name = parts
                java_table_path = self._gateway.jvm.com.alibaba.fluss.metadata.TablePath.of(database_name, table_name)
            else:
                raise ValueError("Table path must be in format 'database.table'")
        else:
            if hasattr(table_path, 'to_java'):
                java_table_path = table_path.to_java(self._gateway)
            else:
                java_table_path = table_path
        
        def drop_operation():
            return self._java_admin.dropTable(java_table_path, not if_exists)  # ignoreIfNotExists = !if_exists
        
        self._connection_manager._handle_async_operation_with_retry(drop_operation)
        return True
