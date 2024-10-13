import unittest
from unittest.mock import patch
import threading
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import queue
from shoots import ShootsServer, ShootsClient, PutMode, BucketDeleteMode
from pyarrow.flight import Location
# import logging
# logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

class QueueTest(unittest.TestCase):
    def setUp(self):
        # Setup server and client as specified
        location = Location.for_grpc_tcp("localhost", 8085)
        self.server = ShootsServer(location, "queuebucket")
        self.write_client1 = ShootsClient("localhost", 8085)
        self.write_client2 = ShootsClient("localhost", 8085)
        self.write_client3 = ShootsClient("localhost", 8085)
        self.read_client = ShootsClient("localhost", 8085)

    def tearDown(self):
        self.write_client1.delete_bucket("test_bucket", BucketDeleteMode.DELETE_CONTENTS)

    def test_concurrent_large_writes_with_delay(self):
        """
        Test that three clients writing large dataframes concurrently results in
        all data being written correctly with a delay added to each write.
        """

        num_rows = 1000000
        dataframe1 = pd.DataFrame({'column1': range(num_rows)})
        dataframe2 = pd.DataFrame({'column1': range(num_rows, 2 * num_rows)})
        dataframe3 = pd.DataFrame({'column1': range(2 * num_rows, 3 * num_rows)})

        # Function to simulate each client writing its dataframe
        def client_write(write_client, dataframe):
            write_client.put(
                name='test_large_write',
                dataframe=dataframe,
                mode=PutMode.APPEND,
                bucket='test_bucket',
                batch_size=100000  # Adjust as needed
                )

        def multi_bucket_job():
            for i in range(0,250):
                self.write_client1.put("df", 
                                       dataframe1,
                                       bucket="another_bucket",
                                       mode=PutMode.REPLACE)
                df = self.read_client.get("df",bucket="another_bucket")
                self.assertEqual(len(df), 1000000)
                
        def read_job():
            for i in range(0,500):
                try:
                    # read_client.get('test_large_write', bucket="test_bucket")
                    if i % 2:
                        sql = "select * from test_large_write limit 1"
                    else:
                        sql = None
                    self.read_client.get('test_large_write', bucket="test_bucket",
                            sql=sql) 
                except FileNotFoundError as e:
                    pass # most likely the read job simply got to the file first


        # Create threads for each client
        threads = [
            threading.Thread(target=client_write, args=(self.write_client1, dataframe1)),
            threading.Thread(target=client_write, args=(self.write_client2, dataframe2)),
            threading.Thread(target=client_write, args=(self.write_client3, dataframe3)),
            threading.Thread(target=read_job),
            threading.Thread(target=multi_bucket_job)]
        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        result_dataframe = self.write_client1.get('test_large_write', bucket="test_bucket")
        # Verify that the total number of rows matches the expected sum
        expected_num_rows = num_rows * 3
        actual_num_rows = len(result_dataframe)
        self.assertEqual(actual_num_rows, expected_num_rows, 
                         f"Expected {expected_num_rows} rows, but found {actual_num_rows} rows.")

        # Optionally, verify that the data is contiguous (no missing or duplicated ranges)
        expected_data = pd.concat([dataframe1, dataframe2, dataframe3]).sort_values('column1').reset_index(drop=True)
        pd.testing.assert_frame_equal(result_dataframe.sort_values('column1').reset_index(drop=True), 
                                      expected_data,
                                      check_like=True)
        

if __name__ == '__main__':
    unittest.main()
