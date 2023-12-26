from pydantic import BaseModel, ValidationError, validator, root_validator
from pydantic_settings import BaseSettings
from typing import Optional
import pyarrow as pa
from pyarrow.flight import FlightDescriptor, FlightClient, Ticket, Action, FlightError
import pandas as pd
import json
from enum import Enum

class PutMode(Enum):
    """
    Enum for specifying the mode of operation when putting dataframes. Used in ShootsClient.put().

    Attributes:
        REPLACE: Replace existing data.
        APPEND: Append to existing data.
        ERROR: (default) Raise an error if data already exists.
    """
    REPLACE = "replace"
    APPEND = "append"
    ERROR = "error"

class BucketDeleteMode(Enum):
    """
    Enum for specifying the mode of operation when deleting a bucket. Used with ShootsClient.delete_bucket().

    Attributes:
        DELETE_CONTENTS: Delete the bucket and its contents.
        ERROR: (default) Raise an error if the bucket is not empty.
    """
    DELETE_CONTENTS = "delete"
    ERROR = "error"

class ClientConfig(BaseSettings):
    """
    Internal class for configuration for ShootsClient using pydantic BaseSettings for environment management.
    """

    host: str
    port: int

class DeleteRequest(BaseModel):
    """
    Internal class for configuring a delete request.
    """
    name: str

    @validator('name')
    def validate_name(cls, v):
        if not v or not isinstance(v, str):
            raise ValueError('name must be a non-empty string')
        return v

class DeleteBucketRequest(BaseModel):
    """
    Internal class for configuring a delete_bucket request.
    """
    name: str
    mode: BucketDeleteMode = BucketDeleteMode.ERROR

class PutRequest(BaseModel):
    """
    Internal class for configuring a put request
    """
    dataframe: pd.DataFrame
    name: str
    mode: PutMode = PutMode.APPEND
    bucket: Optional[str] = None

    class Config:
        arbitrary_types_allowed = True

    @validator('dataframe')
    def validate_dataframe(cls, v):
        if not isinstance(v, pd.DataFrame):
            raise ValueError('table must be a pandas DataFrame')
        return v

class ListRequest(BaseModel):
    """
    Internal class for configuring a list request.
    """
    bucket: Optional[str] = None
    regex: Optional[str] = None

class ResampleRequest(BaseModel):
    """
    Internal class for configuring a resample request.
    """
    source: str
    target: str
    rule: Optional[str] = None
    time_col: Optional[str] = None
    aggregation_func: Optional[str] = None
    mode: PutMode = PutMode.APPEND
    source_bucket: Optional[str] = None
    target_bucket_bucket: Optional[str] = None
    sql: Optional[str] = None

    @root_validator(pre=True)
    def check_sql_and_fields(cls, values):
        sql = values.get('sql')
        rule = values.get('rule')
        time_col = values.get('time_col')
        aggregation_func = values.get('aggregation_func')

        if sql is None:
            if rule is None or time_col is None or aggregation_func is None:
                raise ValueError("rule, time_col, and aggregation_func are required if sql is not provided")
        else:
            values['rule'] = None
            values['time_col'] = None
            values['aggregation_func'] = None

        return values

class GetRequest(BaseModel):
    """
    Internal class for configuring a get request.
    """
    name: str
    sql: Optional[str] = None
    bucket: Optional[str] = None

    @validator('name')
    def validate_name(cls, v):
        if not v or not isinstance(v, str):
            raise ValueError('name must be a non-empty string')
        return v

class ShootsClient:
    """
    Client class for interacting with a ShootsServer instance.

    Methods:
        __init__: Constructor for creating a ShootsClient instance.
        put: Sends a PUT request to the server.
        get: Sends a GET request to the server and retrieves data.
        buckets: Retrieves a list of all buckets from the server.
        delete_bucket: Deletes a specified bucket from the server.
        list: Lists dataframes from the server.
        shutdown: Requests the server to shutdown.
        delete: Sends a request to the server to delete a dataframe.
        _flight_result_to_list: Internal method to convert Flight result to list.
        _flight_result_to_string: Internal method to convert Flight result to string.
    """
    def __init__(self, host: str, port: int):
        """
        Initializes the ShootsClient with the specified host and port.

        This method creates a new instance of the ShootsClient, setting up the
        connection to the FlightServer. It configures the client with the provided
        host and port parameters.

        Args:
            host (str): The hostname or IP address of the FlightServer.
            port (int): The port number on which the FlightServer is listening.

        Raises:
            ValidationError: If the provided host or port values are not valid 
                            or if the FlightClient cannot be configured with 
                            the given host and port.
        
        Example:
            To create a client instance that connects to a FlightServer running
            on localhost at port 8081, use the following:

            ```python
            client = ShootsClient("localhost", 8081)
            ```

        Note:
            The client uses gRPC for communication with the FlightServer. Ensure
            that the FlightServer is running and accessible at the specified host
            and port before creating the ShootsClient instance.
        """
        try:
            config = ClientConfig(host=host, port=port)
            self.client = FlightClient(f"grpc://{config.host}:{config.port}")
        except ValidationError as e:
            print(f"Configuration error: {e}")
            raise

    def put(self, 
            name: str, 
            dataframe: pd.DataFrame, 
            mode: PutMode = PutMode.ERROR,
            bucket: Optional[str] = None):
        """
        Sends a dataframe to the server to be stored or appended to an existing dataframe.

        This method allows the client to send a pandas DataFrame to the FlightServer. 
        The data can either replace an existing dataframe, be appended to it, or trigger 
        an error if the dataframe already exists, based on the specified mode. The data 
        is sent to a specified bucket, which is a logical grouping or directory on the 
        server.

        Args:
            name (str): The name of the datafra e to which the data will be written.
            dataframe (pd.DataFrame): The pandas DataFrame containing the data to be sent.
            mode (PutMode): The mode of operation when writing the data. The default mode 
                            is ERROR, which will raise an error if the dataframe already exists. 
                            Other modes are REPLACE and APPEND.
            bucket (Optional[str]): The name of the bucket where the dataframe will be stored. 
                                    If None, a default bucket may be used.

        Raises:
            ValidationError: If the provided arguments are not valid or if there is a 
                            problem with the DataFrame format.
            FlightServerError: If the dataframe already exists and the put mode was set to ERROR.
            FlightServerError: Other problems encountered on the server while trying to write.

        Example:
            To send a DataFrame 'df' to the server and store it as 'my_dataframe' in the 
            'my_bucket' bucket, use the following:

            ```python
            client = ShootsClient("localhost", 8080)
            client.put(name="my_dataframe", dataframe=df, mode=PutMode.REPLACE, bucket="my_bucket")
            ```
        
        Note:
            If no bucket is specified, the dataframe will be available in the default, unnamed, bucket.

            The first time a put() is called with a bucket name, the bucket will be automatically created.
        """
        try:
            req = PutRequest(dataframe=dataframe, name=name, mode=mode, bucket=bucket)

            command = json.dumps({"name": req.name,
                                 "mode": req.mode.value,
                                 "bucket":bucket}).encode()
            
            descriptor = FlightDescriptor.for_command(command)
            table = pa.Table.from_pandas(req.dataframe)

            writer, _ = self.client.do_put(descriptor, table.schema)
            writer.write_table(table)
            writer.close()

        except ValidationError as e:
            print(f"Validation error: {e}")

    def get(self, name: str, sql: Optional[str] = None, bucket: Optional[str] = None):
        """
        Retrieves a dataframe from the server based on the specified dataframe name, optional SQL query, and bucket.

        This method requests data from the FlightServer. It can retrieve an entire dataframe
        or a subset of it if an SQL query is provided. The data is returned as a pandas DataFrame.
        If a bucket is specified, it retrieves the data from that particular bucket.

        Args:
            name (str): The name of the dataframe to retrieve.
            sql (Optional[str]): An optional SQL query string to filter the dataframe. If None, 
                                the entire dataframe is retrieved.
            bucket (Optional[str]): The name of the bucket where the dataframe is stored. If None, 
                                    a default bucket is assumed.

        Returns:
            pd.DataFrame: A DataFrame containing the retrieved data.

        Raises:
            ValidationError: If the provided arguments are not valid or if the request 
                            cannot be processed by the server.
            FlightServerError: An error is encountered on the server, such as the specified dataframe cannot be found.

        Example:
            To retrieve a dataframe named 'my_dataframe' from the server, and filter it using an 
            SQL query, use the following:

            ```python
            client = ShootsClient("localhost", 8080)
            df = client.get(name="my_dataframe", sql="SELECT * FROM my_dataframe WHERE condition", bucket="my_bucket")
            ```
        """
        try:
            req = GetRequest(name=name, sql=sql, bucket=bucket)
            ticket_data = {"name":req.name, "bucket":req.bucket}
            if sql is not None:
                ticket_data["sql"] = req.sql

            ticket_bytes = json.dumps(ticket_data)
            ticket = Ticket(ticket_bytes)
            reader = self.client.do_get(ticket)
            return reader.read_all().to_pandas()
        
        except ValidationError as e:
            print(f"Validation error: {e}")

    def buckets(self):
        """
        Retrieves a list of all available buckets from the server.

        Buckets are logical groupings or directories in which
        dataframes are stored on the server. The method returns a list of bucket names.

        Returns:
            list: A list of strings, each representing a bucket name.

        Example:
            To get a list of all buckets from the server, use the following:

            ```python
            client = ShootsClient("localhost", 8080)
            bucket_list = client.buckets()
            print(bucket_list)
            ```
        """
        action_obj = {}
        bytes = json.dumps(action_obj).encode()
        action = Action("buckets",bytes)
        result = self.client.do_action(action)
        return self._flight_result_to_list(result)
    
    def delete_bucket(self, name: str, mode: BucketDeleteMode = BucketDeleteMode.ERROR):
        """
        Sends a request to the server to delete a specified bucket.

        The deletion can be performed in different modes: either deleting the bucket and its
        contents, or raising an error if the bucket is not empty, based on the specified mode.

        Args:
            name (str): The name of the bucket to be deleted.
            mode (BucketDeleteMode): The mode of deletion. Default is ERROR, which raises an 
                                    error if the bucket is not empty. DELETE_CONTENTS mode 
                                    deletes the bucket and its contents.

        Returns:
            str: A message indicating the result of the deletion operation.

        Raises:
            FlightServerError: If the server encounters an error processing the deletion request.

        Example:
            To delete a bucket named 'my_bucket' and its contents, use the following:

            ```python
            client = ShootsClient("localhost", 8080)
            result_message = client.delete_bucket(name="my_bucket", mode=BucketDeleteMode.DELETE_CONTENTS)
            print(result_message)
            ```
        """
        req = DeleteBucketRequest(name=name, mode=mode)
        action_obj = {"name":req.name, "mode":req.mode.value}
        bytes = json.dumps(action_obj).encode()
        action = Action("delete_bucket",bytes)
        result = self.client.do_action(action)
        return self._flight_result_to_string(result)       

    def list(self, bucket: Optional[str] = None, regex: Optional[str] = None):
        """
        Lists dataframes available on the server, optionally filtered by a specific bucket.

        Each dataframe is returned with its name and schema.

        Args:
            bucket (Optional[str]): The name of the bucket to filter dataframes. If None, dataframes from the default bucket are listed.
            regex (Optional[str]): Currently ignored.

        Returns:
            list[dict]: A list of dictionaries, each containing the 'name' and 'schema' of a dataframe.

        Example:
            To list all dataframe in the 'my_bucket' bucket, use the following:

            ```python
            client = ShootsClient("localhost", 8080)
            dataframes = client.list(bucket="my_bucket")
            for dataframe in dataframes:
                print(dataframe["name"], dataframe["schema"])
            ```

        Note:
            The method returns an empty list if no dataframes match the filtering criteria or if 
            the server does not have any dataframes. The 'schema' in the returned dictionary is 
            an Apache Arrow schema object.
        """
        descriptor_data = {"bucket":bucket, "regex":regex}
        descriptor_bytes = json.dumps(descriptor_data).encode()
        flights = self.client.list_flights(criteria=descriptor_bytes)
        dataframes = []
        for flight in flights:
            dataframes.append({"name":flight.descriptor.path[0].decode(), "schema":flight.schema})
        return dataframes

    def shutdown(self):
        """
        Sends a shutdown request to the server.

        This method is used to send a command to the FlightServer to initiate its shutdown
        procedure. It is useful for programmatically stopping the server from the client side.

        Returns:
            flight.Result: The result of the shutdown action, typically a confirmation message.

        Example:
            To send a shutdown command to the server, use the following:

            ```python
            client = ShootsClient("localhost", 8080)
            shutdown_result = client.shutdown()
            print(shutdown_result)
            ```

        Note:
            This method should be used with caution as it will stop the server and terminate 
            all ongoing operations.
        """
        action_description = json.dumps({}).encode()
        action = Action("shutdown",action_description)
        result = self.client.do_action(action)
        return self._flight_result_to_string(result)

    def delete(self, name: str, bucket: Optional[str] = None):
        """
        Sends a request to the server to delete a specific dataframe, optionally within a 
        specified bucket. 

        Args:
            name (str): The name of the dataframe to be deleted.
            bucket (Optional[str]): The name of the bucket containing the dataframe. If None, 
                                    the default bucket or a server-defined location is used.

        Returns:
            str: A message indicating the result of the delete operation.

        Raises:
            FlightServerError: If the server encounters an error processing the delete request.

        Example:
            To delete a dataframe named 'my_dataframe' from the server, use the following:

            ```python
            client = ShootsClient("localhost", 8081)
            delete_result = client.delete(name="my_dataframe", bucket="my_bucket")
            print(delete_result)
            ```

        Note:
            Be cautious when using this method as deleting a dataframe is irreversible. Ensure 
            that the dataframe is correctly specified.
        """
        action_description = json.dumps({"name":name, "bucket":bucket}).encode()
        action = Action("delete",action_description)
        result = self.client.do_action(action)

        return self._flight_result_to_string(result)
    
    def resample(self, 
                source: str, 
                target: str, 
                rule: Optional[str] = None, 
                time_col: Optional[str] = None,
                aggregation_func: Optional[str] = None,
                sql: Optional[str] = None,
                mode: Optional[PutMode] = PutMode.APPEND,
                source_bucket: Optional[str] = None,
                target_bucket: Optional[str] = None):
        """
        Resamples (a.k.a. downsamples) data on the server. Works with a provided SQL query, or, if the data is time series,
        callers can supply a rule, time column, and aggregation function.

        ```resample()``` does not require a round trip of the data from the server, but rather performs
        the operation on the server.

        Args:
            source (str): The name of the dataframe to resample
            target (str):  The name of the resampled dataframe
            sql (str): A sql stream for selecting data from the source dataframe
            mode (Optional[PutMode]): Behavior if a target dataframe already exists (defults to APPEND)
            source_bucket (Optional[str]): Bucket containing the source dataframe, if any
            target_bucket (Optional[str]): Bucket for where to store the resampled dataframe, if any

            The following arguments are ignoed if the sql argument is provide, and required if not.
            rule (str): String representation of time delta for windowing (example: 1s)
            time_col (str): The name of the time stamp column to window on
            aggregation_func (str): The name of the function to aggregate (examples: mean, max, count)


        Raises:
            FlightServerError
        
        Example:
            ```python
            self.client.resample(source="my_source_dataframe", 
                                target="my_resampled_dataframe",
                                rule="10s",
                                time_col="timestamp",
                                aggregation_func="mean",
                                mode=PutMode.APPEND)
            ```

        """

        req = ResampleRequest(
                source=source,
                target=target,
                sql=sql,
                rule=rule,
                time_col=time_col,
                aggregation_func=aggregation_func,
                mode=mode,
                source_bucket=source_bucket,
                target_bucket=target_bucket)

        resample_data = {"source":req.source,
                "target":req.target,
                "mode":mode.value,
                "source_bucket":source_bucket,
                "target_bucket":target_bucket}
        
        if(sql):
            resample_data["sql"] = req.sql
        else:
            resample_data["rule"] = req.rule
            resample_data["time_col"] = req.time_col
            resample_data["aggregation_func"] = req.aggregation_func
        
        bytes = json.dumps(resample_data).encode()
        action = Action("resample",bytes)
        result = self.client.do_action(action)
        return json.loads(self._flight_result_to_string(result))
      
    def _flight_result_to_list(self, result):
        list_string = None
        for r in result:
            list_string = r.body.to_pybytes().decode()
        return json.loads(list_string)

    def _flight_result_to_string(self, result):
        msg = ""
        for r in result:
            msg += r.body.to_pybytes().decode() + "\n"
        
        msg = msg.rstrip("\n")
        return msg