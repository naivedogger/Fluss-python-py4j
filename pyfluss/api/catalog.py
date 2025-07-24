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
from typing import Optional, Dict, Any


class Catalog(ABC):
    """
    Abstract base class for Fluss catalog operations.
    """

    @staticmethod
    @abstractmethod
    def create(catalog_options: Dict[str, Any]) -> 'Catalog':
        """
        Create a catalog with the given options.
        
        Args:
            catalog_options: Configuration options for the catalog
            
        Returns:
            A catalog instance
        """
        pass

    @abstractmethod
    def get_table(self, identifier: str) -> 'Table':
        """
        Get a table by identifier.
        
        Args:
            identifier: Table identifier
            
        Returns:
            Table instance
        """
        pass

    @abstractmethod
    def create_database(self, name: str, ignore_if_exists: bool = False, 
                       properties: Optional[Dict[str, Any]] = None):
        """
        Create a database.
        
        Args:
            name: Database name
            ignore_if_exists: Whether to ignore if database already exists
            properties: Database properties
        """
        pass

    @abstractmethod
    def create_table(self, identifier: str, schema: 'Schema', ignore_if_exists: bool = False):
        """
        Create a table.
        
        Args:
            identifier: Table identifier
            schema: Table schema
            ignore_if_exists: Whether to ignore if table already exists
        """
        pass
