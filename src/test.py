from shoots_client import ShootsClient
from shoots_server import ShootsServer
from shoots_client import PutMode, BucketDeleteMode
import pandas as pd
import numpy as np
from pyarrow.flight import FlightServerError, Location, FlightClient
import threading
import shutil
import random
import string
import unittest

class TestClient(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestClient, self).__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls):
        cls.port = 8082
        cls.bucket_dir = "unittest_buckets"

        cls.location = Location.for_grpc_tcp("localhost", cls.port)
        cls.server = ShootsServer(cls.location, bucket_dir=cls.bucket_dir)
        cls.server_thread = threading.Thread(target=cls.server.serve)
        cls.server_thread.start()

        cls.client = ShootsClient("localhost", cls.port)
        data = {"col1":[0],"col2":["zero"]}
        cls.dataframe0 = pd.DataFrame(data)
        data = {"col1":[1],"col2":["one"]}
        cls.dataframe1 = pd.DataFrame(data)

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server_thread.join()

        shutil.rmtree(cls.bucket_dir)  # Clean up the directory

    def test_list_actions(self):
        client = FlightClient(self.location)
        actions = client.list_actions()
        self.assertGreaterEqual(len(actions), 3)
    
    def test_write_replace_mode(self):
        self.client.put("test1",self.dataframe0,mode=PutMode.REPLACE)    
        self.client.put("test1",self.dataframe1,mode=PutMode.REPLACE)
        res = self.client.get("test1")
        self.assertEqual(res.shape[0],1)
        self.assertEqual(res.iloc(0)[0].col1, 1)
        
        self.client.delete("test1")

    def test_delete_file_not_found(self):
        with self.assertRaises(FlightServerError):
            self.client.delete("abcdefghijklmnopqrstuvwxyz")
        
    def test_write_error(self):
        self.client.put("test1",self.dataframe1,mode=PutMode.ERROR)
        with self.assertRaises(FlightServerError):
            self.client.put("test1",self.dataframe1,mode=PutMode.ERROR)
        self.client.delete("test1")

    def test_write_append(self):
        self.client.put("test1",self.dataframe0,mode=PutMode.ERROR)
        self.client.put("test1",self.dataframe1,mode=PutMode.APPEND)
        res = self.client.get("test1")
        self.assertEqual(res.shape[0],2)

        self.client.delete("test1")

    def test_read_with_select_star(self):
        self.client.put("test1",self.dataframe0,mode=PutMode.ERROR)
        self.client.put("test1",self.dataframe1,mode=PutMode.APPEND)  
        sql = "SELECT * FROM test1"
        res = self.client.get("test1", sql)
        self.assertEqual(res.shape[0],2)
        self.client.delete("test1")

    def test_read_with_select(self):
        self.client.put("test1",self.dataframe0,mode=PutMode.ERROR)
        self.client.put("test1",self.dataframe1,mode=PutMode.APPEND)  
        sql = "SELECT col2 FROM test1 where col1 = 1"
        res = self.client.get("test1", sql)
        self.assertEqual(res.shape[0],1)
        self.client.delete("test1")
    
    def test_read_write_to_bucket(self):
        bucket = "test_bucket"
        self.client.put("test1",self.dataframe0,mode=PutMode.REPLACE,bucket=bucket)
        res = self.client.get("test1",bucket=bucket)
        self.assertEqual(res.shape[0],1)
        self.client.delete("test1", bucket=bucket)

    def test_list_and_delete_bucket(self):
        bucket = "testing_bucket"
        self.client.put("test1",self.dataframe0,mode=PutMode.REPLACE,bucket=bucket)
        buckets = self.client.buckets()
        self.assertIn(bucket, buckets)
        with self.assertRaises(FlightServerError):
            self.client.delete_bucket("test1", mode=BucketDeleteMode.ERROR)

        self.client.delete_bucket(bucket, mode=BucketDeleteMode.DELETE_CONTENTS)
        buckets = self.client.buckets()
        self.assertNotIn(bucket, buckets)

    def test_list(self):
        self.client.put("test1",self.dataframe0,mode=PutMode.REPLACE)
        self.client.put("test2",self.dataframe0,mode=PutMode.REPLACE)
        
        files_count = len(self.client.list())
        
        try:
            self.assertEqual(files_count, 2)
        except:
            self.client.delete("test1")
            self.client.delete("test2")
            raise
        self.client.delete("test1")
        self.client.delete("test2")

    def test_resample_with_sql(self):
        df = self._generate_dataframe(1000000)
        source = "million0"
        self.client.put(name=source, dataframe=df)

        sql = f"SELECT * FROM {source} LIMIT 10"
        res = self.client.resample(source=source,
                                   target="ten",
                                   sql=sql)
        self.assertEqual(res["target_rows"], 10)

        df = self.client.get("ten")
        self.assertEqual(df.shape[0], 10)
        self.client.delete(source)
        self.client.delete("ten")

    def test_resample_no_buckets(self):
        num_rows = 1000000
        df = self._generate_dataframe_with_timestamp(num_rows)

        self.client.put(name="million", dataframe=df)

        res = self.client.resample(source="million", 
                             target="thousand",
                             rule="10s",
                             time_col="timestamp",
                             aggregation_func="mean"
                             )
        
        self.assertEqual(res["target_rows"], 1000)

        df_thousands = self.client.get("thousand")
        self.assertEqual(df_thousands.shape[0], 1000)    

        self.client.delete("million")
        self.client.delete("thousand")

    def test_resample_append(self):
        num_rows = 1000000
        df = self._generate_dataframe_with_timestamp(num_rows)

        self.client.put(name="million", dataframe=df)

        res = self.client.resample(source="million", 
                             target="thousand",
                             rule="10s",
                             time_col="timestamp",
                             aggregation_func="mean")
        
        self.assertEqual(res["target_rows"], 1000)

        self.client.resample(source="million", 
                             target="thousand",
                             rule="10s",
                             time_col="timestamp",
                             aggregation_func="mean",
                             mode=PutMode.REPLACE)

        df_thousands = self.client.get("thousand")
        self.assertEqual(df_thousands.shape[0], 1000)    


        self.client.resample(source="million", 
                             target="thousand",
                             rule="10s",
                             time_col="timestamp",
                             aggregation_func="mean",
                             mode=PutMode.APPEND)

        df_thousands = self.client.get("thousand")
        self.assertEqual(df_thousands.shape[0], 2000) 

        self.client.delete("million")
        self.client.delete("thousand")

    def test_resample_with_buckets(self):
        num_rows = 1000000
        df = self._generate_dataframe_with_timestamp(num_rows)

        source_bucket="million_bucket"
        target_bucket="thousand_bucket"
        self.client.put(name="million", dataframe=df, bucket=source_bucket)

        res = self.client.resample(source="million", 
                             target="thousand",
                             rule="10s",
                             time_col="timestamp",
                             aggregation_func="mean",
                             source_bucket=source_bucket,
                             target_bucket=target_bucket)
        
        self.assertEqual(res["target_rows"], 1000)

        df_thousands = self.client.get("thousand",bucket=target_bucket)
        self.assertEqual(df_thousands.shape[0], 1000)    

        self.client.delete("million", bucket=source_bucket)
        self.client.delete("thousand", bucket=target_bucket)

    def _generate_dataframe(self, num_rows):
        integers = np.random.randint(0, 100, size=num_rows)  # Random integers between 0 and 99
        floats = np.random.random(size=num_rows)  # Random floats
        strings = [''.join(random.choices(string.ascii_lowercase, k=5)) for _ in range(num_rows)]  # Random 5-letter strings

        # Create the DataFrame
        return pd.DataFrame({
            'int_col': integers,
            'float_col': floats,
            'string': strings})
        


    def _generate_dataframe_with_timestamp(self, num_rows):
        date_range_milliseconds = pd.date_range(start='2020-01-01', 
                                                periods=num_rows, 
                                                freq='10L')

        df = pd.DataFrame({
            'timestamp': date_range_milliseconds,
            'data': np.random.randn(num_rows)  # Example data column with random numbers
        })
        
        return df

    def test_list_with_bucket(self):
        self.client.put("test1",
                        self.dataframe0,
                        mode=PutMode.REPLACE,
                        bucket="listybucket")
        self.client.put("test2",
                        self.dataframe0,
                        mode=PutMode.REPLACE,
                        bucket="listybucket")

        datasets = self.client.list(bucket="listybucket")
        files_count = len(datasets)
        names = []
        for dataset in datasets:
            names.append(dataset["name"])
        try:
            self.assertEqual(files_count, 2)
            self.assertIn("test1", names)
            self.assertIn("test2", names)
        except:
            self.client.delete("test1",
                        bucket="listybucket")
            self.client.delete("test2",
                        bucket="listybucket")
            raise
        
        self.client.delete("test1",
                        bucket="listybucket")
        self.client.delete("test2",
                        bucket="listybucket")
        
if __name__ == '__main__':
    unittest.main()