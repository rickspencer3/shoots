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
import sys

use_tls = False

class TestClient(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestClient, self).__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls):
        cls.port = 8082
        cls.bucket_dir = "unittest_buckets"
        if use_tls:
            cls.location = Location.for_grpc_tls("localhost", cls.port)
            cls.server = ShootsServer(cls.location,
                                      bucket_dir=cls.bucket_dir,
                                      certs=(server_cert,server_key))
        else: 
            cls.location = Location.for_grpc_tcp("localhost", cls.port)
            cls.server = ShootsServer(cls.location, bucket_dir=cls.bucket_dir)

        cls.server_thread = threading.Thread(target=cls.server.serve)
        cls.server_thread.start()

        cls.client = ShootsClient("localhost", 
                                  cls.port, 
                                  use_tls,
                                  root_cert)
        data = {"col1":[0],"col2":["zero"]}
        cls.dataframe0 = pd.DataFrame(data)
        data = {"col1":[1],"col2":["one"]}
        cls.dataframe1 = pd.DataFrame(data)

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server_thread.join()

        shutil.rmtree(cls.bucket_dir)  # Clean up the directory

    # def test_list_actions(self):
    #         kwargs = {}
    #         if not use_tls:
    #             url_scheme = "grpc://"
    #         else:
    #             url_scheme = "grpc+tls://"
    #             kwargs["tls_root_certs"] = root_cert
    #         url = f"{url_scheme}localhost:8082"
    #         self.client = FlightClient(url, **kwargs)

    #         client = FlightClient(self.location, tls_options=client_options)
    #     else:
    #         client = FlightClient(self.location)
    
    #     actions = client.list_actions()
    #     self.assertGreaterEqual(len(actions), 3)

    
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

server_cert = """-----BEGIN CERTIFICATE-----
MIIDrDCCApSgAwIBAgIUD+l0waPPxuBtzvr6Rw221CugWIwwDQYJKoZIhvcNAQEL
BQAwYDELMAkGA1UEBhMCdXMxCzAJBgNVBAgMAm1kMRIwEAYDVQQHDAlyb2Nrdmls
bGUxDTALBgNVBAoMBHNlbGYxDTALBgNVBAsMBHNlbGYxEjAQBgNVBAMMCWxvY2Fs
aG9zdDAeFw0yNDAxMjAyMzA0MjZaFw0yNTA2MDMyMzA0MjZaMGAxCzAJBgNVBAYT
AnVzMQswCQYDVQQIDAJtZDESMBAGA1UEBwwJcm9ja3ZpbGxlMQ0wCwYDVQQKDARz
ZWxmMQ0wCwYDVQQLDARzZWxmMRIwEAYDVQQDDAlsb2NhbGhvc3QwggEiMA0GCSqG
SIb3DQEBAQUAA4IBDwAwggEKAoIBAQC2zmFFr73t+Lob9TAbVB+D14PyugObA0KO
LI5Ta1KZKNBjJ3In5yCbP4BipggzQIi77wjoRnzVwT0VVCqmCqh2wxpdG6snsqcj
XbXDMfrf2e4ojoo2DY4mmk7XQWGJEU89Uoj+ha7+Nz+rgBfjR1jk2nCF5f4hjWGp
HFryCvxgNdxsqw06+ETXcmPQtaoa8fVqYKu9r06sGC8EgzAKTdsq4KHqQTskpmqN
CSeFMIXlt2eFhtfC7hOAOkbw+IKHFPOEXVm6FK+OYt95Dt+TIpCCxRnOM/e9KNGc
6QvRzmAA0Ohf68EaZIjFXBbZxS7f6VJ9RC1f5VSKvuTvQyrwu9otAgMBAAGjXjBc
MBoGA1UdEQQTMBGCCWxvY2FsaG9zdIcEfwAAATAdBgNVHQ4EFgQUJIdGG820rPoV
14bBGtdl9FChbL0wHwYDVR0jBBgwFoAUExYiJmioMTbQzqFIy6J1/WXF0EEwDQYJ
KoZIhvcNAQELBQADggEBAJpbaiIh+t9vjT45kE/PR9DQjfhMOjC49mWl1eFLfY+C
fCICXD2UBrI265gdGlLCRiVU2HUNFP6AxaLW87zzJ69yIafgYXkAo1JPxdpQbBNq
n8gOLDkBLIdVGVCqFwG3HHaxMMf291yIVVJ7lX0ZQjuUNX0iAuL7fP/sdM2X+mZ5
oFtluuCNQWOPCHcssa/CIN/IZH7+9NtXxjisxng1xyGEZHKMGB1Dn2okqpP3zDlq
gOtqomX0d7LCb0fSTsBoXbINexYGt3mUemiaC59J7jpJHjARPTCY4N5BdPeYWfyI
3+RZlIfHY2N/WCrxVZ5hnJnynYhOQU637mhR9MOg3RI=
-----END CERTIFICATE-----"""

server_key = """-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC2zmFFr73t+Lob
9TAbVB+D14PyugObA0KOLI5Ta1KZKNBjJ3In5yCbP4BipggzQIi77wjoRnzVwT0V
VCqmCqh2wxpdG6snsqcjXbXDMfrf2e4ojoo2DY4mmk7XQWGJEU89Uoj+ha7+Nz+r
gBfjR1jk2nCF5f4hjWGpHFryCvxgNdxsqw06+ETXcmPQtaoa8fVqYKu9r06sGC8E
gzAKTdsq4KHqQTskpmqNCSeFMIXlt2eFhtfC7hOAOkbw+IKHFPOEXVm6FK+OYt95
Dt+TIpCCxRnOM/e9KNGc6QvRzmAA0Ohf68EaZIjFXBbZxS7f6VJ9RC1f5VSKvuTv
Qyrwu9otAgMBAAECggEAS0HDQdELu8A9vey+lUGYh8pdg7uYaGR7LdSh8y2nn23x
/B/tGwhHoCIQVIxNq+vRXq0VWapLEiSvZOx1ck3I9Lt1Swds+rbNn/2gKm+U/DrX
lNK5i28hKjs0YeGEqcz6diBYTY/zbqyIBa6CJwrkSbKzyCva5y0xG1GVDzf+lgxi
yL6+UJtJmv5bhWegEipe3SOlb5DsUKIQGwZbEYMMaEYfM/jbIlnSLWNh+EB3Ep2z
N76JmgbH7TXpq16J9aTaVsQfggHNkfxmdzDMhFZOKZZQdfOLW+5OaoN/LXzT3L8Q
NR2VfNEIARwlnmv7vmkYOtarVJnCrWjm9oETiFkg8QKBgQD2QmmCLy2ivdRzEP55
A4elaQvm6j8olHFip0zJ/wEToLTV6032GazHb+W8WhSGmD3RAtTGAPJg8tfC4ofs
oRU3UroVJpbwWhBnx1YfhbfFYjmTIkgo4Xk34l5Auk2DhIL+i7mOPkrKXJvPPO+v
peUGJM5tWcYzR92AmlhQWuYs7wKBgQC+CXMBlSuCRd8o2OMg5/MW0Cgl/uk5KyMq
q2mUKNiVfBqlh1GOVvM3yYx7Q64z4LD7whmiF3O6Qc0Gh70qCARdnbUfZh+pr7rf
9TYAVfVgvjOa5gdZqTquBrxYYqb+BaHb0nid3gzenKIBJUN3nPR7+n8HO4zwFKgK
Mk2NkCKiowKBgQCthjaaiWv61RCy1DK8SwR2RgMg/8bpbVBIV+MAIzQ7BG7onhod
ZBIfpnWXt1HnVbeZZAlSTLB+KCBpLv7getFcXmrJJJwPWSdeOVQyeiL4bzJqvylh
xR+JhXUs3xpnpiQ4AXULClHhLkiMS0AJ4Tf0kFL1MuE6mVU3nYrFdIfk+QKBgBmt
PyvMkvdkJ3XmcDHnBTJk57fSjIKb8IF4baiGKFVyUkthUESyPOShx+hzyZra1i7+
F+aN0qYs893ZA1clhCl+AJYAf3C2/MH76NL8yk3LBT/9qqqTsgkHgfVTRwr1idwg
wQlbklOVyFHtTgNgYqxJuVYp1q4trFLMXywseHGPAoGAHVl2Sz7JWBilMVtGURmM
X3Cq1nQxf820+PQO06ebLUAqgSo3lloVybNFzzRGTmbtHjR6HkVKAfAkf3rj02uc
AlQB2BliRZIVqE5J8ZhGA1VSSHDQcVnSEoaL6VSdKhuNvUMoJbDAIOc5/6AxDg5c
W6wn/4QNjcFCLK7IxkU+mi0=
-----END PRIVATE KEY-----"""

root_cert = """-----BEGIN CERTIFICATE-----
MIIDozCCAougAwIBAgIUEz+VqfyowWmoV3qCaOpMmPSaV1IwDQYJKoZIhvcNAQEL
BQAwYDELMAkGA1UEBhMCdXMxCzAJBgNVBAgMAm1kMRIwEAYDVQQHDAlyb2Nrdmls
bGUxDTALBgNVBAoMBHNlbGYxDTALBgNVBAsMBHNlbGYxEjAQBgNVBAMMCWxvY2Fs
aG9zdDAgFw0yNDAxMjAyMjU3MTlaGA8yMDUxMDYwNzIyNTcxOVowYDELMAkGA1UE
BhMCdXMxCzAJBgNVBAgMAm1kMRIwEAYDVQQHDAlyb2NrdmlsbGUxDTALBgNVBAoM
BHNlbGYxDTALBgNVBAsMBHNlbGYxEjAQBgNVBAMMCWxvY2FsaG9zdDCCASIwDQYJ
KoZIhvcNAQEBBQADggEPADCCAQoCggEBAK4sJWgQNAVpEcAvQD3Je1KAnZIzhnZU
jnbgER8ruF/nssg8lChclTyTEJIScCVzNDcUiMqDqlqhKDrslzNcTrXWQfnpPlSP
RU8SXdKKOX1+CMZ2I+zATUo/i1XBcgLCp0n74VEpxgP51vZMkuaYnLqIBxJ5z+uF
sJsZi3BwzDB2CwSgCVY6XXZFrp2lLw15yXKX/wtJ/rZzzWDH30kGDGS5K8CkfRN9
E2zQteKl3bdgaVMyii9vr/KB74C+nk9126U3GALVOpn1pCbt/cWM0Z2MBzyr3/RS
DAyukghgz5bx/le2ThBo67c0nbr1PFZ+ZcmFAuiOa28QxrhIwTqnzH8CAwEAAaNT
MFEwHQYDVR0OBBYEFBMWIiZoqDE20M6hSMuidf1lxdBBMB8GA1UdIwQYMBaAFBMW
IiZoqDE20M6hSMuidf1lxdBBMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQEL
BQADggEBADSFiONPT/kKnqrGTMD5Ehvi6m/gyhRUKBeO8Zk4xRYl5mLp3Gq8YqOk
k2IHuFyRSWDaOt2SGyHyfi9kA9MC3eigGYB8jbRqJRPumTkGpbyr0Ao8cHt9Eghh
bZMxcQdpzbVv4q/zyD1jc/eE47yy8ddm1RIR3Xx5zKVuLHNvV9MXJs/bmWZDlH2G
szsMWLGz/HjezdNh5FMmyj60fEOQerjf+FZBZNYFk//siLKnnJJMVABNVXYk4i+7
lZNEMvLlQ/j48ORH46+4lutnDTdTGIiyHroaq2XFHqNay4jfrl7FZwwlJ18jfj+J
3FosYGMK6Jm+Zqrg4fK8yeu8ozGOhII=
-----END CERTIFICATE-----"""

if __name__ == '__main__':
    no_tls = False
    tls_only = False

    if "--no-tls" in sys.argv:
        no_tls = True
        sys.argv.remove("--no-tls")
    
    if "--tls-only" in sys.argv:
        tls_only = True
        sys.argv.remove("--tls-only")
    
    if no_tls:
        use_tls = False
        unittest.main()

    elif tls_only:
        use_tls = True
        unittest.main()
    else:
        use_tls = False
        unittest.main(exit=False)

        use_tls = True
        unittest.main()
