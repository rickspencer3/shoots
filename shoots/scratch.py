from shoots_client import ShootsClient
from shoots_client import PutMode, BucketDeleteMode
import pandas as pd
import numpy as np
from pyarrow.flight import FlightError

shoots = ShootsClient("localhost", 8081)

num_rows = 1000000
date_range_milliseconds = pd.date_range(start='2020-01-01', periods=num_rows, freq='10L')

# Creating a DataFrame with the date range
df = pd.DataFrame({
    'timestamp': date_range_milliseconds,
    'data': np.random.randn(num_rows)  # Example data column with random numbers
})

print(df.head())
df['timestamp'] = pd.to_datetime(df['timestamp'])
df.set_index('timestamp', inplace=True)
print(f"source_cols {df.shape[0]}")

method = "mean"
resampled_df = df.resample('10s')
aggregation_method = getattr(resampled_df, method, None)

df_target = aggregation_method()
print(f"source_cols {df_target.shape[0]}")



# df = pd.read_csv('sensor_data.csv')
# print(df)
# df['Timestamp'] = pd.to_datetime(df['Timestamp'])
# df.set_index('Timestamp', inplace=True)

# print(df.resample('100ms').mean())

# shoots.put("sensor_data", dataframe=df, mode=PutMode.APPEND, bucket="my-bucket")
# res = shoots.delete_bucket("my-bucket", mode=BucketDeleteMode.DELETE_CONTENTS)
# print(res)
# print(shoots.shutdown())
# for r in res:
#     print(r.body.to_pybytes().decode())

# df1 = shoots.get("sensor_data", bucket="my-bucket")
# print(df1)

# print("buckets before deletion:")
# print(shoots.buckets())
# shoots.delete_bucket("my-bucket", mode=BucketDeleteMode.DELETE_CONTENTS)
# print("buckets after deletion:")
# print(shoots.buckets())

# sql = 'select "Sensor_1" from sensor_data where "Sensor_2" < .2'
# df1 = shoots.get("sensor_data", sql=sql)
# print(df1)

# results = shoots.list()
# print("dataframes stored:")
# for r in results:
#     print(r["name"])

# shoots.delete("sensor_data")
# df = pd.read_csv('sensor_data.csv')

# earliest_time = df['Timestamp'].min()
# latest_time = df['Timestamp'].max()
# print("Earliest Time:", earliest_time)
# print("Latest Time:", latest_time)

# column_means = df.mean(numeric_only=True)
# print(column_means)

# shoots = ShootsClient("localhost", 8081)
# shoots.put("sensor_data", dataframe=df, mode=PutMode.REPLACE)

# sql = 'SELECT MEAN("Sensor_1") from sensor_data'

# df0 = shoots.get("sensor_data", sql=sql)
# print(df0)

# print(shoots.list())
# print(shoots.delete("sensor_data"))