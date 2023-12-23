Shoots is dataset storage server. Currently is supports pandas, but most likely I will add support for polars in the future.

Shoots is entirely written in Python and is designed for Python users.

Shoots comes in 2 parts, a server and a client library.

Shoots is very very early software, but is in a usable state.

# shoots_server
The server tries to be a fairly faithful Apache Flight Server, meaning that you should be able to use the Apache Arrow Flight client libraries directly. It is entirely built upon the upstream [Apache Arrow project](https://arrow.apache.org/).

Under the hood, the server receives and server pandas dataframes, storing thenm on disk in Apache Parquet forma. 

# shoots_client
The client pieces wrap the Apache FlightClient to offer an interface for pandas developers, abstracting away the Apache Arrow and Flight concepts.

# usage
## run the server
There are currently no run time options, so running the server is a simple matter of running the python module, depending on your system:

```bash
python shoots_server.py
```
or 

```bash
python3 shoots_server.py
```
## storing a dataframe
Use the client library to create an instance of the client, and "put" a dataframe. Assuming you are running locally:
```python
from shoots_client import ShootsClient
from shoots_client import PutMode
import pandas as pd

shoots = ShootsClient("localhost", 8081)
df = pd.read_csv('sensor_data.csv')

shoots.put("sensor_data", dataframe=df, mode=PutMode.REPLACE)
```
## retrieving data
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

Shoots use [Apache DataFusion](https://arrow.apache.org/datafusion/) for executing SQL. The [DataFusion dialect is well document](https://arrow.apache.org/datafusion/user-guide/sql/index.html).

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
You can delete buckets with the ```delete_bucket()``` method. You can force a deletion of all contents using ```BucketDeleteMode.DELETE_CONTENTS```:

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