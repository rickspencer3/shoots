from shoots_client import ShootsClient
from shoots_client import PutMode
import pandas as pd
from pyarrow.flight import FlightServerError

import unittest

class TestClient(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestClient, self).__init__(*args, **kwargs)
        self.client = ShootsClient("localhost", 8081)
        data = {"col1":[0],"col2":["zero"]}
        self.dataframe0 = pd.DataFrame(data)
        data = {"col1":[1],"col2":["one"]}
        self.dataframe1 = pd.DataFrame(data)

    def test_write_replace_mode(self):
        self.client.put("test1",self.dataframe0,mode=PutMode.REPLACE)    
        self.client.put("test1",self.dataframe1,mode=PutMode.REPLACE)
        res = self.client.get("test1")
        self.assertEqual(res.shape[0],1)
        self.assertEqual(res.iloc(0)[0].col1, 1)

        self.client.delete("test1")

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

    def test_list_with_bucket(self):
        self.client.put("test1",
                        self.dataframe0,
                        mode=PutMode.REPLACE,
                        bucket="listybucket")
        self.client.put("test2",
                        self.dataframe0,
                        mode=PutMode.REPLACE,
                        bucket="listybucket")

        files = self.client.list(bucket="listybucket")
        files_count = len(files)
        
        try:
            self.assertEqual(files_count, 2)
            self.assertIn("test1", files)
            self.assertIn("test2", files)
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