from shoots import PutMode, BucketDeleteMode, ShootsServer, ShootsClient
import pandas as pd
import numpy as np
from pyarrow.flight import Location
import threading
import shutil
import unittest

class LargeDatasetsTest(unittest.TestCase):
    port = 8085
    bucket_dir = "large_datasets_buckets"
    dataset_name = "onehundredmillion"
    def __init__(self, *args, **kwargs):
        super(LargeDatasetsTest, self).__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls):
        location = Location.for_grpc_tcp("localhost", cls.port)
        cls.server = ShootsServer(location, bucket_dir=cls.bucket_dir)
        cls.server_thread = threading.Thread(target=cls.server.serve)
        cls.server_thread.start()

        cls.client = ShootsClient("localhost", cls.port)

        n_rows = 100_000_000
        n_cols = 10

        print("generating large data set")
        small_data = np.random.rand(100, n_cols)
        data = np.tile(small_data, (n_rows // small_data.shape[0], 1))
        cls.large_df = pd.DataFrame(data, columns=[f'column_{i}' for i in range(1, n_cols + 1)])
        print(cls.large_df.head())

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server_thread.join()

        print("cleaning up test data")
        shutil.rmtree(cls.bucket_dir)
    
    def test_put_get_query_large(self):
        print("writing test data with defaults")
        self.client.put(self.dataset_name, dataframe=self.large_df)
        print("retrieving all data")
        df = self.client.get(self.dataset_name)
        records_count = len(df)
        print(f"Retrieved {records_count} records")
        self.assertEqual(records_count, 100000000)

        print("Running sql query")
        sql = f"SELECT * FROM {self.dataset_name} WHERE column_1 < .2 LIMIT 100"
        df = self.client.get(self.dataset_name, sql=sql)
        print("Retrived data:")
        print(df.head())
        self.assertEqual(len(df),100)

        self.client.delete(self.dataset_name)
    
    def test_batch_size(self):
        batch_sizes = [1000000]#, 5000, 10000]
        
        for batch_size in batch_sizes:
            print(f"testing put() for {batch_size}")
            self.client.put(self.dataset_name, 
                            dataframe=self.large_df,
                            batch_size=batch_size)
            sql = f"SELECT * FROM {self.dataset_name} LIMIT 1"
            df = self.client.get(self.dataset_name, sql=sql)
            self.assertEqual(len(df),1)
            self.client.delete(self.dataset_name)
            

