Shoots is dataframe storage server. Currently is supports pandas, but most likely I will add support for polars in the future.

Shoots is entirely written in Python and is designed for Python users.

Shoots comes in 2 parts, a server and a client library.

Shoots is very early software, but is in a usable state. It is stored on [github](https://github.com/rickspencer3/shoots). Issues and contributions welcome.

# shoots_server
The server tries to be a fairly faithful Apache Flight Server, meaning that you should be able to use the Apache Arrow Flight client libraries directly. It is entirely built upon the upstream [Apache Arrow project](https://arrow.apache.org/).

Under the hood, the server receives and serves pandas dataframes, storing thenm on disk in [Apache Parquet](https://parquet.apache.org/) format. However, shoots is designed so that, as a user, you don't need to know about the underlying storage formats and libraries.

# shoots_client
The client pieces wrap the [Apache FlightClient](https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightClient.html) to offer an interface for pandas developers, abstracting away the Apache Arrow and Flight concepts.

# usage
## starting up the server
### terminal
Running the server is a simple matter of running the python module, depending on your system:

```bash
python shoots_server.py
```
or 

```bash
python3 shoots_server.py
```

### start up options
ShootsServer supports the follow CLI arguments:
 - ```--port```: Port number to run the Flight server on. Defaults to 8081.
 - ```--bucket_dir```: Path to the bucket directory. Defaults to ./buckets
 - ```--host```: Host IP address for where the server will run. Defaults to localhost.

To enable TLS on the server, provide an SSL certificate and key.
 - ```--cert_file```: Path to file for cert file for TLS. Defaults to None. 
 - ```--key_file```: Path to file for key file for TLS. Defaults to None.

These options can also be set via environment variables.
 - ```SHOOTS_PORT```
 - ```SHOOTS_BUCKET_DIR```
 - ```SHOOTS_HOST```
 - ```SHOOTS_CERT_FILE```
 - ```SHOOTS_KEY_FILE```

```bash
python3 shoots_server.py --port=8082 --bucket_dir="/foo/bar"
```

### python
You can also start up the server in Python. It is best to start it on a thread or you won't be able to cleanly shut it down.
```python
from pyarrow.flight import Location
import threading

location = Location.for_grpc_tcp("localhost", 8081)
server = ShootsServer(location, bucket_dir="/foo/bar") #bucket_dir is optional
server_thread = threading.Thread(target=server.run)
server_thread.start()
```
## shutting down the server
### from the shoots client
Shoots supports a ```shutdown``` action. You can call it from the shoots client:

```python
from shoots_client import ShootsClient

shoots = ShootsClient("localhost", 8081)
shoots.shutdown()
```
### from python code
The Shoots shutdown function handles the threading, so you can simply call it:

```python
server.shutdown()
```

## creating a client object
ShootsClient requires a host name and port number:
```python
shoots = ShootsClient("localhost", 8081)
```

You can enable tls by setting use TLS to True.
```python
shoots = ShootsClient("localhost", 8081, True)
```

If the server is using a self signed certicate for TLS, you need to provide the root certificate to the client. You do this by passing in the certificate a string.
```python

root_cert = ""
with open(path_to_root_cert) as root_cert_file:
    root_cert = root_cert_file.read()

shoots = ShootsClient("localhost", 8081, True, root_cert)
```

## storing a dataframe
Use the client library to create an instance of the client, and ```put()``` a dataframe. Assuming you are running locally:
```python
from shoots_client import ShootsClient
from shoots_client import PutMode
import pandas as pd

shoots = ShootsClient("localhost", 8081)
df = pd.read_csv('sensor_data.csv')

shoots.put("sensor_data", dataframe=df, mode=PutMode.REPLACE)
```
## retrieving a dataframe
You can simply get a dataframe back by using its name:
```python
df0 = shoots.get("sensor_data")
print(df0)
```
You can also submit a sql query to bring back a subset of the data:
```python
sql = 'select "Sensor_1" from sensor_data where "Sensor_2" < .2'
df1 = shoots.get("sensor_data", sql=sql)
print(df1)
```

Shoots use [Apache DataFusion](https://arrow.apache.org/datafusion/) for executing SQL. The [DataFusion dialect](https://arrow.apache.org/datafusion/user-guide/sql/index.html) is well document.

## listing dataframes
You can retrieve a list of dataframes and their schemas, using the ```list()``` method.

```python
results = shoots.list()
print("dataframes stored:")
for r in results:
    print(r["name"])
```
------
```bash
dataframes stored:
sensor_data
```
## deleting dataframes
You can delete a dataframe using the ```delete()``` method:
```
shoots.delete("sensor_data")
```

## resampling dataframes
You can resample (aka "downsample") dataframes on the server by sending either a command for a time series dataframe, or just send SQL for any arbitrary dataframe.

### resample with SQL
```python
self.client.resample(source="my_source_dataframe", 
                    target="my_resampled_dataframe",
                    sql="SELECT * FROM my_source_dataframe LIMIT 10",
                    mode=PutMode.APPEND)
```         
### resample time series
```python
self.client.resample(source="my_source_dataframe", 
                    target="my_resampled_dataframe",
                    rule="10s",
                    time_col="timestamp",
                    aggregation_func="mean",
                    mode=PutMode.APPEND)
```
## buckets
You can organize your dataframes in buckets. This is essentially a directory where your dataframes are stored. 

### creating buckets
Buckets are implicitly created as needed if you use the "bucket" parameter in ```put()```:

```python
shoots.put("sensor_data", dataframe=df, mode=PutMode.REPLACE, bucket="my-bucket")
df1 = shoots.get("sensor_data", bucket="my-bucket")
print(df1)
```

### listing buckets
You can use the ```buckets()``` method to list available buckets:

```python
print("buckets:")
print(shoots.buckets())
```
------
```bash
buckets:
['my-bucket', 'foo']
```

### deleting buckets
You can delete buckets with the ```delete_bucket()``` method. You can force a deletion of all the dataframes contained in a bucket by using ```BucketDeleteMode.DELETE_CONTENTS```, otherwise you need to delete all of the dataframes first.:

```python
print("buckets before deletion:")
print(shoots.buckets())
shoots.delete_bucket("my-bucket", mode=BucketDeleteMode.DELETE_CONTENTS)
print("buckets after deletion:")
print(shoots.buckets())
```
------
```bash
buckets before deletion:
['my-bucket', 'foo']
buckets after deletion:
['foo']
```

# Roadmap
I intend to work on the following in the coming weeks, in no particular order:

- [X] add a runtime option for the root bucket directory, use it for testing
- [ ] pip packaging
- [ ] pattern matching for ```list()```
- [X] downsampling via sql on the server
- [ ] combining dataframes on the server
- [X] compressing and cleaning dataframes on the server
- [ ] authentication
- [ ] UI with SQL tree view browser and editor
