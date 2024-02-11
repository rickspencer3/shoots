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
        cls.large_df = pd.DataFrame(data, columns=[f'Column_{i}' for i in range(1, n_cols + 1)])

        print("writing test data ...")

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server_thread.join()

        print("deleting test data")
        shutil.rmtree(cls.bucket_dir)
    
    def test_put_get_query_large(self):
        self.client.put("100Million", dataframe=self.large_df)
        df = self.client.get("100Million")
        records_count = len(df)
        print(f"retrieved {records_count} records")
        self.assertEqual(records_count, 100000000)



