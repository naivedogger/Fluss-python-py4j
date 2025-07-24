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
from typing import List, TYPE_CHECKING, Any

if TYPE_CHECKING:
    try:
        import pandas as pd
        import pyarrow as pa
    except ImportError:
        pd = None
        pa = None


class TableWrite(ABC):
    """
    Abstract base class for table write operations.
    """
    pass


class BatchTableWrite(TableWrite):
    """
    Abstract base class for batch table write operations.
    """

    @abstractmethod
    def write_arrow(self, table: Any):
        """
        Write an Arrow table.
        
        Args:
            table: PyArrow Table to write
        """
        pass

    @abstractmethod
    def write_arrow_batch(self, record_batch: Any):
        """
        Write an Arrow record batch.
        
        Args:
            record_batch: PyArrow RecordBatch to write
        """
        pass

    @abstractmethod
    def write_pandas(self, dataframe: Any):
        """
        Write a Pandas DataFrame.
        
        Args:
            dataframe: Pandas DataFrame to write
        """
        pass

    @abstractmethod
    def close(self):
        """
        Close the write operation.
        """
        pass
