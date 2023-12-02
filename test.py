from shoots_client import ShootsClient
from shoots_client import PutMode
import pandas as pd

import unittest


class TestClient(unittest.TestCase):
    def test_write_modes(self):
        data = {"col1":[0],"col2":["zero"]}
        df = pd.DataFrame(data)
        client = ShootsClient("localhost", 8081)
        client.put("test1",df,mode=PutMode.REPLACE)
        res = client.get("test1")
        self.assertEqual(res.shape[0],1)

        data = {"col1":[1],"col2":["one"]}
        df = pd.DataFrame(data)
        client.put("test1",df,mode=PutMode.IGNORE)
        res = client.get("test1")
        self.assertEqual(res.shape[0],1)

        data = {"col1":[1],"col2":["one"]}
        df = pd.DataFrame(data)
        client.put("test1",df,mode=PutMode.APPEND)
        res = client.get("test1")
        self.assertEqual(res.shape[0],2)

        client.delete("test1")

if __name__ == '__main__':
    unittest.main()