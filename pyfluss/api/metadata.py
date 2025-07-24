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

from typing import Dict, Optional


class DatabaseDescriptor:
    """
    Descriptor for database metadata including name, comment, and custom properties.
    """
    
    def __init__(self, comment: Optional[str] = None, custom_properties: Optional[Dict[str, str]] = None):
        """
        Initialize a DatabaseDescriptor.
        
        Args:
            comment: Optional comment for the database
            custom_properties: Optional custom properties as key-value pairs
        """
        self._comment = comment
        self._custom_properties = custom_properties or {}
    
    @property
    def comment(self) -> Optional[str]:
        """Get the database comment."""
        return self._comment
    
    @property
    def custom_properties(self) -> Dict[str, str]:
        """Get the custom properties."""
        return self._custom_properties.copy()
    
    def with_comment(self, comment: str) -> 'DatabaseDescriptor':
        """
        Return a new DatabaseDescriptor with the specified comment.
        
        Args:
            comment: Database comment
            
        Returns:
            New DatabaseDescriptor instance
        """
        return DatabaseDescriptor(comment=comment, custom_properties=self._custom_properties)
    
    def with_custom_property(self, key: str, value: str) -> 'DatabaseDescriptor':
        """
        Return a new DatabaseDescriptor with an additional custom property.
        
        Args:
            key: Property key
            value: Property value
            
        Returns:
            New DatabaseDescriptor instance
        """
        new_properties = self._custom_properties.copy()
        new_properties[key] = value
        return DatabaseDescriptor(comment=self._comment, custom_properties=new_properties)
    
    def with_custom_properties(self, properties: Dict[str, str]) -> 'DatabaseDescriptor':
        """
        Return a new DatabaseDescriptor with the specified custom properties.
        
        Args:
            properties: Custom properties dictionary
            
        Returns:
            New DatabaseDescriptor instance
        """
        new_properties = self._custom_properties.copy()
        new_properties.update(properties)
        return DatabaseDescriptor(comment=self._comment, custom_properties=new_properties)
    
    @staticmethod
    def builder() -> 'DatabaseDescriptorBuilder':
        """
        Create a new DatabaseDescriptor builder.
        
        Returns:
            DatabaseDescriptorBuilder instance
        """
        return DatabaseDescriptorBuilder()
    
    def __repr__(self):
        return f"DatabaseDescriptor(comment={self._comment!r}, custom_properties={self._custom_properties!r})"


class DatabaseDescriptorBuilder:
    """
    Builder for DatabaseDescriptor instances.
    """
    
    def __init__(self):
        self._comment = None
        self._custom_properties = {}
    
    def comment(self, comment: str) -> 'DatabaseDescriptorBuilder':
        """
        Set the database comment.
        
        Args:
            comment: Database comment
            
        Returns:
            This builder instance
        """
        self._comment = comment
        return self
    
    def custom_property(self, key: str, value: str) -> 'DatabaseDescriptorBuilder':
        """
        Add a custom property.
        
        Args:
            key: Property key
            value: Property value
            
        Returns:
            This builder instance
        """
        self._custom_properties[key] = value
        return self
    
    def custom_properties(self, properties: Dict[str, str]) -> 'DatabaseDescriptorBuilder':
        """
        Set custom properties.
        
        Args:
            properties: Custom properties dictionary
            
        Returns:
            This builder instance
        """
        self._custom_properties.update(properties)
        return self
    
    def build(self) -> DatabaseDescriptor:
        """
        Build the DatabaseDescriptor.
        
        Returns:
            DatabaseDescriptor instance
        """
        return DatabaseDescriptor(comment=self._comment, custom_properties=self._custom_properties)


class TablePath:
    """
    Represents a table path in the format database.table or catalog.database.table.
    """
    
    def __init__(self, database_name: str, table_name: str, catalog_name: Optional[str] = None):
        """
        Initialize a TablePath.
        
        Args:
            database_name: Database name
            table_name: Table name
            catalog_name: Optional catalog name
        """
        self._catalog_name = catalog_name
        self._database_name = database_name
        self._table_name = table_name
    
    @property
    def catalog_name(self) -> Optional[str]:
        """Get the catalog name."""
        return self._catalog_name
    
    @property
    def database_name(self) -> str:
        """Get the database name."""
        return self._database_name
    
    @property
    def table_name(self) -> str:
        """Get the table name."""
        return self._table_name
    
    @staticmethod
    def of(database_name: str, table_name: str) -> 'TablePath':
        """
        Create a TablePath with database and table name.
        
        Args:
            database_name: Database name
            table_name: Table name
            
        Returns:
            TablePath instance
        """
        return TablePath(database_name=database_name, table_name=table_name)
    
    @staticmethod
    def of_catalog(catalog_name: str, database_name: str, table_name: str) -> 'TablePath':
        """
        Create a TablePath with catalog, database and table name.
        
        Args:
            catalog_name: Catalog name
            database_name: Database name
            table_name: Table name
            
        Returns:
            TablePath instance
        """
        return TablePath(database_name=database_name, table_name=table_name, catalog_name=catalog_name)
    
    def to_string(self) -> str:
        """
        Convert to string representation.
        
        Returns:
            String representation of the table path
        """
        if self._catalog_name:
            return f"{self._catalog_name}.{self._database_name}.{self._table_name}"
        else:
            return f"{self._database_name}.{self._table_name}"
    
    def __str__(self):
        return self.to_string()
    
    def __repr__(self):
        return f"TablePath(catalog_name={self._catalog_name!r}, database_name={self._database_name!r}, table_name={self._table_name!r})"
