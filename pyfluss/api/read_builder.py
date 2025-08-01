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
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from .table_scan import TableScan
    from .table_read import TableRead  
    from .row_type import RowType


class ReadBuilder(ABC):
    """
    Abstract base class for read builder operations.
    """

    @abstractmethod
    def with_projection(self, projection: List[str]) -> 'ReadBuilder':
        """
        Apply column projection to the read operation.
        
        Args:
            projection: List of column names to project
            
        Returns:
            ReadBuilder instance for chaining
        """
        pass

    @abstractmethod
    def with_limit(self, limit: int) -> 'ReadBuilder':
        """
        Apply a limit to the read operation.
        
        Args:
            limit: Maximum number of records to read
            
        Returns:
            ReadBuilder instance for chaining
        """
        pass

    @abstractmethod
    def new_scan(self) -> 'TableScan':
        """
        Create a new table scan.
        
        Returns:
            TableScan instance
        """
        pass

    @abstractmethod
    def new_read(self) -> 'TableRead':
        """
        Create a new table read.
        
        Returns:
            TableRead instance
        """
        pass

    @abstractmethod
    def read_type(self) -> 'RowType':
        """
        Get the row type for the read operation.
        
        Returns:
            RowType instance
        """
        pass
