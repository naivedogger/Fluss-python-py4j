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
from typing import List, Dict, Any, TYPE_CHECKING

if TYPE_CHECKING:
    pass


class TableScan(ABC):
    """
    Abstract base class for table scan operations.
    """

    @abstractmethod
    def plan(self) -> 'Plan':
        """
        Create an execution plan for the scan.
        
        Returns:
            Plan instance
        """
        pass


class Plan(ABC):
    """
    Abstract base class for execution plan in Fluss.
    """

    @abstractmethod
    def table_buckets(self) -> List[Dict[str, Any]]:
        """
        Get the table buckets for this plan.
        
        Returns:
            List of table bucket dictionaries containing bucket metadata
        """
        pass
