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
from typing import List, Optional, Iterator, Any, Dict
import pandas as pd
import pyarrow as pa


class FlussTableBucket:
    """
    Represents a Fluss table bucket (data partition unit).
    This is the actual concept used in Fluss, not Split.
    """
    
    def __init__(self, table_id: int, bucket_id: int):
        self.table_id = table_id
        self.bucket_id = bucket_id
    
    def __str__(self):
        return f"FlussTableBucket(table_id={self.table_id}, bucket_id={self.bucket_id})"
    
    def __repr__(self):
        return self.__str__()


class FlussTableRead(ABC):
    """
    Abstract base class for Fluss table read operations.
    
    This is designed specifically for Fluss architecture, which uses
    TableBucket concept instead of Split.
    """

    @abstractmethod
    def read_records(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Read records from the table.
        
        Args:
            limit: Maximum number of records to read
            
        Returns:
            List of records as dictionaries
        """
        pass

    @abstractmethod
    def read_batch(self, batch_size: int = 1000) -> Iterator[List[Dict[str, Any]]]:
        """
        Read records in batches.
        
        Args:
            batch_size: Number of records per batch
            
        Yields:
            Batches of records
        """
        pass

    def to_pandas(self, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Convert table data to Pandas DataFrame.
        
        Args:
            limit: Maximum number of records to read
            
        Returns:
            Pandas DataFrame
        """
        try:
            records = self.read_records(limit)
            if not records:
                return pd.DataFrame()
            
            # Convert records to DataFrame
            return pd.DataFrame(records)
            
        except ImportError:
            raise ImportError("pandas is required for to_pandas(). Install with: pip install pandas")

    def to_arrow(self, limit: Optional[int] = None) -> pa.Table:
        """
        Convert table data to PyArrow Table.
        
        Args:
            limit: Maximum number of records to read
            
        Returns:
            PyArrow Table
        """
        try:
            # First convert to pandas, then to arrow
            df = self.to_pandas(limit)
            return pa.Table.from_pandas(df)
            
        except ImportError:
            raise ImportError("pyarrow is required for to_arrow(). Install with: pip install pyarrow")

    def to_arrow_batch_reader(self, batch_size: int = 1000) -> pa.RecordBatchReader:
        """
        Convert table data to Arrow batch reader.
        
        Args:
            batch_size: Number of records per batch
            
        Returns:
            Arrow RecordBatchReader
        """
        try:
            def batch_generator():
                for batch_records in self.read_batch(batch_size):
                    if batch_records:
                        batch_df = pd.DataFrame(batch_records)
                        yield pa.RecordBatch.from_pandas(batch_df)
            
            # Get schema from first batch
            first_batch_records = next(self.read_batch(1), [])
            if not first_batch_records:
                # Empty table
                schema = pa.schema([])
            else:
                sample_df = pd.DataFrame(first_batch_records)
                schema = pa.Schema.from_pandas(sample_df)
            
            return pa.RecordBatchReader.from_batches(schema, batch_generator())
            
        except ImportError:
            raise ImportError("pyarrow is required for to_arrow_batch_reader(). Install with: pip install pyarrow")

    def to_duckdb(self, table_name: str, connection: Optional[Any] = None) -> Any:
        """
        Convert table data to DuckDB table.
        
        Args:
            table_name: Name for the table in DuckDB
            connection: Optional DuckDB connection
            
        Returns:
            DuckDB connection
        """
        try:
            import duckdb
            con = connection or duckdb.connect(database=":memory:")
            arrow_table = self.to_arrow()
            con.register(table_name, arrow_table)
            return con
            
        except ImportError:
            raise ImportError("duckdb is required for to_duckdb(). Install with: pip install duckdb")

    def to_ray(self) -> Any:
        """
        Convert table data to Ray Dataset.
        
        Returns:
            Ray Dataset
        """
        try:
            import ray
            arrow_table = self.to_arrow()
            return ray.data.from_arrow(arrow_table)
            
        except ImportError:
            raise ImportError("ray is required for to_ray(). Install with: pip install ray")

    @abstractmethod
    def to_record_generator(self) -> Iterator[Dict[str, Any]]:
        """
        Convert table data to record generator.
        
        Returns:
            Record generator iterator
        """
        pass

    def count(self) -> int:
        """
        Count total number of records in the table.
        
        Returns:
            Total record count
        """
        count = 0
        for batch in self.read_batch():
            count += len(batch)
        return count

    def sample(self, n: int = 10) -> List[Dict[str, Any]]:
        """
        Get a sample of records from the table.
        
        Args:
            n: Number of sample records
            
        Returns:
            Sample records
        """
        return self.read_records(limit=n)

    def schema_info(self) -> Dict[str, Any]:
        """
        Get schema information from sample data.
        
        Returns:
            Schema information dictionary
        """
        sample_records = self.sample(1)
        if not sample_records:
            return {"columns": [], "dtypes": {}}
        
        sample_record = sample_records[0]
        dtypes = {}
        for key, value in sample_record.items():
            dtypes[key] = type(value).__name__
        
        return {
            "columns": list(sample_record.keys()),
            "dtypes": dtypes,
            "sample_record": sample_record
        }


class FlussTableReadImpl(FlussTableRead):
    """
    Concrete implementation of FlussTableRead.
    
    This bridges our high-level API with the actual Fluss reader.
    """
    
    def __init__(self, fluss_reader):
        """
        Initialize with a Fluss reader instance.
        
        Args:
            fluss_reader: The underlying Fluss data reader
        """
        self._fluss_reader = fluss_reader
    
    def read_records(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Read records using the Fluss reader."""
        return self._fluss_reader.read_rows(limit or 1000)
    
    def read_batch(self, batch_size: int = 1000) -> Iterator[List[Dict[str, Any]]]:
        """Read records in batches using the Fluss reader."""
        while True:
            batch = self._fluss_reader.read_rows(batch_size)
            if not batch:
                break
            yield batch
    
    def to_record_generator(self) -> Iterator[Dict[str, Any]]:
        """Generate records one by one."""
        for batch in self.read_batch():
            for record in batch:
                yield record
