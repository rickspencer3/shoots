import os
import pyarrow as pa
from pyarrow import flight
import pyarrow.parquet as pq
from datafusion import SessionContext
import json
import shutil
import threading
import argparse

put_modes = ["error", "append", "replace"]

class ShootsServer(flight.FlightServerBase):
    """
    A FlightServer for managing and serving pandas datasets.

    Attributes:
        location (pyarrow.flight.Location): The server location.
        bucket_dir (str): Directory path for storing parquet datasets.

    Note:
        You most likely don't want to use the server directly, except for starting it up. It is easiest to interact with the server via ShootsClient.
    """

    def __init__(self, 
                 location,
                 bucket_dir,
                 certs = None,
                 *args, **kwargs):
        """
        Initializes the ShootsServer.

        Args:
            location (pyarrow.flight.Location): The location where the server will run.
            bucket_dir (str): Directory path where the parquet files will be stored.
            certs (tuple of str): An TLS certificate and key (in that order) for providing TLS support for the server.
            If no certs are provided, the server will run without TLS.
        """
        self.location = location
        self.bucket_dir = bucket_dir
        if certs == None:
            super(ShootsServer, self).__init__(location, *args, **kwargs)
        else:
            super(ShootsServer, self).__init__(location,
                                    None, # auth_handler
                                    [certs],
                                    False, # verify_client
                                    *args, **kwargs)
            
    def do_get(self, context, ticket):
        """
        Handles the retrieval of a dataframe based on the given ticket.

        You can optionally specify a bucket where the dataframe is stored.

        You can optionally specify a SQL statement which will use the DataFusion query engine to query the data before returned the dataframe.

        Args:
            ticket (flight.Ticket): The ticket object containing the dataset request details.

        Returns:
            flight.RecordBatchStream: A stream of record batches for the requested dataset.

        Raises:
            flight.FlightServerError: If there is an issue in processing the request.
        
        Example:
            To request a dataset, create a ticket with the required information in JSON format.

            ```python
            import json
            from pyarrow import flight

            # Define the request details
            # sql and bucket are both optional
            # Leave out the sql statement to return the whole dataframe
            ticket_data = {
                "name": "my_dataset",
                "bucket": "my_bucket",
                "sql": "SELECT * FROM my_dataset WHERE condition"
            }

            # Encode the request as a JSON string
            ticket_bytes = json.dumps(request).encode()

            # Create a ticket object
            ticket = flight.Ticket(ticket_bytes)

            # Use this ticket with the do_get method of the FlightServer
            reader = self.client.do_get(ticket)
            ```
        """
        try:
            ticket_info = json.loads(ticket.ticket.decode())
            name = ticket_info["name"]
            bucket = ticket_info["bucket"]
            file_path = self._create_file_path(name, bucket)
            if "sql" in ticket_info:
                sql_query = ticket_info["sql"]
                table = self._get_arrow_table_from_sql(name, file_path, sql_query)
                
                return flight.RecordBatchStream(table)
            
            else:
                table = pq.read_table(file_path)
                return flight.RecordBatchStream(table)
            
        except Exception as e:
            raise flight.FlightServerError(extra_info=str(e))

    def _get_arrow_table_from_sql(self, name, file_path, sql_query):
        ctx = SessionContext()
        ctx.register_parquet(name, file_path)
                
        result = ctx.sql(sql_query)
        table = result.to_arrow_table()
        return table
    
    def do_put(self, context, descriptor, reader, writer):
        """
        Handles uploading or appending data to a dataframe.
        
        You can optionally specify a mode to determine the behavior in case there is already a dataframe with the same name stored:
         - error - (default) The put operation will return a FlightServerError, and no changes will take place
         - append - Add the data in the dataframe to the existing dataframe of the same name.
         - replace - Delete all of the data in the existing dataframe and replace it with the new data.

         You can optionally specific a bucket. A bucket is top level organization for your dataframes. Buckets will be created automatically on write if needed.
        Args:
            descriptor (flight.FlightDescriptor): Descriptor containing details about the dataset.

        Raises:
            flight.FlightServerError: If an error occurs during data processing.
        
        Example:
            To write dataset, create a Ticket with the required information in JSON format. 

            ```python
            # create the json for the FlightDescriptor. Mode and bucket are optional.
            descriptor_bytes = json.dumps({"name": "my_bucket",
                        "mode": "error", 
                        "bucket":"my_bucket"}).encode()
            
            # create the descriptor
            descriptor = FlightDescriptor.for_command(command)

            # convert the dataframe to an arrow table
            table = pa.Table.from_pandas(df)

            # call do_put() to get back a writer
            writer, _ = self.client.do_put(descriptor, table.schema)
            
            # write the data and close the writer
            writer.write_table(table)
            writer.close()
            ```
        """
        command_info = json.loads(descriptor.command.decode())

        # Extract name and mode from the command
        name = command_info["name"]
        mode = command_info["mode"]
        bucket = command_info["bucket"]

        self._raise_if_invalid_put_mode(mode)
        data_table = reader.read_all()
        self._write_arrow_table(name, mode, bucket, data_table) 

    def _raise_if_invalid_put_mode(self, mode):
        if mode not in put_modes:
            raise flight.FlightServerError(f"put mode is {mode}, must be one of {put_modes}")

    def _write_arrow_table(self, name, mode, bucket, data_table):
        file_path = self._create_file_path(name, bucket)
        if os.path.exists(file_path):
            if mode == "append":
                existing_table = pq.read_table(file_path)
                data_table = pa.concat_tables([data_table, existing_table])
                pq.write_table(data_table, file_path)
            
            elif(mode == "error"):
                raise flight.FlightServerError(f"File {name} Exists", extra_info="File Exists")
            else:
                pq.write_table(data_table, file_path)
        else:
            pq.write_table(data_table, file_path)

    def list_flights(self, context, criteria):
        """
        Lists available dataframes based on given criteria.

        You can optionally specify a bucket name to list dataframes in the specified bucket.

        Args:
            criteria: Criteria to filter datasets.

        Yields:
            flight.FlightInfo: Information about each available flight (dataset).
        
        Example:
            ```python
                # create the criteria. bucket can be None
                criteria_data = {"bucket":"my_bucket", "regex":None}
                criateria_bytes = json.dumps(descriptor_data).encode()

                # get the list of FlightInfos.
                flights = self.client.list_flights(criteria=descriptor_bytes)

                # iterate the FlightInfos
                for flight in flights:
                    print(flight.descriptor.path[0].decode(), flight.schema)                
            ```

        Note:
            The regex criteria is not yet implemented on the server.
        """

        criteria_info = json.loads(criteria.decode())
        bucket = criteria_info["bucket"]
        regex = criteria_info["regex"]
        files = self._list_parquet_files(bucket)
        for file in files:
            file_path = os.path.join(self.bucket_dir, file)
            if bucket:
                file_path = os.path.join(self.bucket_dir, bucket, file)
            parquet_file = pq.ParquetFile(file_path)
            schema = parquet_file.schema.to_arrow_schema()
            descriptor = flight.FlightDescriptor.for_path(file[:-8])
            yield flight.FlightInfo(schema,
                            descriptor,
                            [],  # No endpoints, replace with actual data locations if applicable
                            parquet_file.metadata.num_rows,
                            parquet_file.metadata.serialized_size)

    def do_action(self, context, action):
        """
        Performs a specific action based on the request.

        do_action() is a generic handler that receives instructions through the action "type" (i.e. name of the action), and a json payload for extra instructions.

        delete_bucket has an optional mode to determine the behavior if there are dataframes in the bucket.
        error - (default) raise a FlightServerError and leave the bucket untouched
        delete - delete all of the contents of the bucket and the bucket itself.

        Args:.
            action (flight.Action): An action to be performed.

        Raises:
            FlightServerError

        Returns:
            Flight Result.

            buckets returns FlightResult with a list of bucket names, others return a message.

        Example:
            The different actions require different payloads.

            ```python
            client = FlightClient(f"grpc://{config.host}:{config.port}")

            # delete example with optional bucket name
            action_description = json.dumps({"name":"my_dataframe", "bucket":"my_bucket"}).encode()
            action = Action("delete",action_description)
            client.do_action(action)

            # buckets example
            bytes = json.dumps({}).encode()
            action = Action("buckets",bytes)
            result = client.do_action(action)
            for r in result:
                print(r.body.to_pybytes().decode())

            # delete_bucket example
            action_obj = {"name":"my_bucket, "mode":"error"}
            bytes = json.dumps(action_obj).encode()
            action = Action("delete_bucket",bytes)
            client.do_action(action)

            # shutdown example
            bytes = json.dumps({}).encode()
            action = Action("shutdown", bytes)
            client.do_action(action)

            # resample time series example
            resample_data = {"source":"my_original_dataframe",
            "target":"my_new_dataframe",
            "rule":"1s",
            "time_col":"my_timestamp_column",
            "aggregation_func":"mean"}
        
            bytes = json.dumps(resample_data).encode()
            action = Action("resample",bytes)
            result = client.do_action(action)

            # resample with a sql query example
            resample_data = {"source":"my_original_dataframe",
            "target":"my_new_dataframe",
            "sql":"SElECT * FROM my_original_dataframe LIMIT 10"}
        
            bytes = json.dumps(resample_data).encode()
            action = Action("resample",bytes)
            result = client.do_action(action)

            # ping
            result = self.shoots_client.ping() # result should be pong

            ```
        """

        action, action_bytes = action.type, action.body.to_pybytes().decode()
        action_info = None
        if action_bytes:
            action_info = json.loads(action_bytes)

        if action == "delete":
            return self._delete(action_info)
        if action == "buckets":
            return self._buckets()
        if action == "delete_bucket":
            return self._delete_bucket(action_info)
        if action == "shutdown":
            return self.shutdown()
        if action == "resample":
            if action_info.get("sql",False):
                return self._resample_with_sql(action_info)
            else:
                return self._resample_time_series(action_info)
        if action == "ping":
            result = flight.Result(b'pong')
            return [result]
    
    def _resample_with_sql(self, resample_info):
        source = resample_info["source"]
        target = resample_info["target"]
        source_bucket = resample_info["source_bucket"]
        target_bucket = resample_info["target_bucket"]
        sql = resample_info["sql"]
        mode = resample_info["mode"]

        self._raise_if_invalid_put_mode(mode)

        target_rows = -1

        file_path = self._create_file_path(source, source_bucket)
        table = self._get_arrow_table_from_sql(source, file_path, sql)
        target_rows = table.num_rows
        self._write_arrow_table(target, mode, target_bucket, table)
        return self._flight_result_from_dict({"target_rows":target_rows})
    
    def _resample_time_series(self, resample_info):
        source = resample_info["source"]
        target = resample_info["target"]
        rule = resample_info["rule"]
        time_col = resample_info["time_col"]
        aggregation_func = resample_info["aggregation_func"]
        source_bucket = resample_info["source_bucket"]
        target_bucket = resample_info["target_bucket"]
        mode = resample_info["mode"]

        source_rows = -1
        target_rows = -1

        df_source = self._load_dataframe_from_file(source, source_bucket)
        source_rows = df_source.shape[0]

        df_source.set_index(time_col, inplace=True)
        df_source = df_source.resample(rule)

        method = getattr(df_source, aggregation_func, None)
        df_target = method()
        target_rows = df_target.shape[0]
 
        table = pa.Table.from_pandas(df_target)
        if mode != "append":    
            self._write_arrow_table(target, mode ,target_bucket, table)
        else:
            file_path = self._create_file_path(target, target_bucket)
            if os.path.exists(file_path):
                existing_table = pq.read_table(file_path)
              
                data_table = pa.concat_tables([existing_table, table])

                pq.write_table(data_table, file_path)
            else:
                self._write_arrow_table(target, mode ,target_bucket, table)

        return self._flight_result_from_dict({"source_rows":source_rows,
                                              "target_rows":target_rows})

    def _load_dataframe_from_file(self, source, source_bucket):
        file_name = self._create_file_path(source, source_bucket)
        
        table = pq.read_table(file_name)
        df_source = table.to_pandas()
        return df_source

    def list_actions(self, context):
        """
        Lists all available actions that the server can perform.

        Args:
            context: The server context.

        Returns:
            List[flight.ActionType]: A list of available actions.
        """

        actions = [
            ("delete", "Delete a dataframe"),
            ("buckets", "List buckets"),
            ("delete_bucket", "Delete a bucket"),
            ("shutdown", "Shutdown the server"),
            ("resample", "Conversion and resampling of time series or with a sql query"),
            ("ping", "Convenience action for testing if the server is functional")
        ]

        return [flight.ActionType(action, description) for action, description in actions]

    def _create_file_path(self, name, bucket=None):
        bucket_path = None
        if bucket: 
            bucket_path = os.path.join(self.bucket_dir, bucket)
        else:
            bucket_path = self.bucket_dir
        os.makedirs(bucket_path, exist_ok=True)

        file_name = f"{name}.parquet"

        file_path = os.path.join(bucket_path, file_name)
        
        return file_path
        
    def _delete_bucket(self, delete_info):
        bucket = delete_info["name"]
        mode = delete_info["mode"]
        bucket_path = os.path.join(self.bucket_dir, bucket)
        if not os.path.isdir(bucket_path):
            raise flight.FlightServerError(f"No such bucket: {bucket}",
                                            extra_info="No Such Bucket")
        
        bucket_is_empty = not os.listdir(bucket_path)
        if not bucket_is_empty:
            if mode == "error":
                raise flight.FlightServerError(f"Bucket Not Empty: {bucket}",
                                                extra_info="Bucket Not Empty")
            elif mode == "delete":
                shutil.rmtree(bucket_path)
        else: 
            os.rmdir(bucket_path) 
        result_info = {"message":f"bucket {bucket} deleted"}
        return self._flight_result_from_dict(result_info)

    def _buckets(self):
        entries = os.listdir(self.bucket_dir)
        buckets = [entry for entry in entries if os.path.isdir(os.path.join(self.bucket_dir, entry))]
        return self._list_to_flight_result(buckets)

    def _list_to_flight_result(self, strings):
        bytes = json.dumps(strings).encode()
        result = flight.Result(bytes)
        return [result]

    def _list_parquet_files(self, bucket):
        bucket_path = None
        if bucket: 
            bucket_path = os.path.join(self.bucket_dir, bucket)
        else:
            bucket_path = self.bucket_dir
            
        all_files = os.listdir(bucket_path)
        parquet_files = [filename for filename in all_files if filename.endswith('.parquet')]
        
        return parquet_files
    
    def _delete(self, delete_info):
        name = delete_info["name"]
        bucket = delete_info["bucket"]
        file_path = self._create_file_path(name, bucket)

        try:
            os.remove(file_path)
        except FileNotFoundError:
            raise flight.FlightServerError(f"Dataframe {name} not found",
                                extra_info="No Such Dataset")
        except PermissionError:
            raise flight.FlightUnauthorizedError(f"Insufficent permisions to delete {name}")
        except OSError as e:
            raise flight.FlightServerError(f"Error encountered deleting {name}",
                                           extra_info=str(e))
        
        result_info = {"success":True, "message":f"deleted {name}"}
        return self._flight_result_from_dict(result_info)

    def _flight_result_from_dict(self, result_info):
        bytes = json.dumps(result_info).encode()
        result = flight.Result(bytes)
        return [result]

    def shutdown(self):
        """
        Gracefully shuts down the server.

        Note:
            shutdown() is not exposed to the FlightClient, but it can be accessed via do_action

        Example:
            ```python
            action = Action("shutdown",json.dumps({}).encode())
            result = self.client.do_action(action)
            for r in res:
                print(r.body.to_pybytes().decode())
                # prints ["shutdown command received"]
            ```
        """
        shutdown_thread = threading.Thread(target=super(ShootsServer, self).shutdown)
        shutdown_thread.start()
        
        print("\nShutting down Shoots server")
        return self._list_to_flight_result(["shutdown command received"])

    def serve(self):
        """
        Serve until shutdown is called.

        """
        print(f"Starting Shoots server on {self.location.uri.decode()}")
        
        super(ShootsServer, self).serve()

def _read_cert_files(cert_file, key_file):
    with open(cert_file, 'r') as cert_file_content:
        cert_data = cert_file_content.read()
    with open(key_file, 'r') as key_file_content:
        key_data = key_file_content.read()
    return(cert_data, key_data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Starts the Shoots Flight Server.')
    parser.add_argument('--port', type=int, default=8081, help='Port number to run the Flight server on.')
    parser.add_argument('--bucket_dir', type=str, default='buckets', help='Path to the bucket directory.')
    parser.add_argument('--host', type=str, default='localhost', help='Host IP address for where the server will run.')
    parser.add_argument('--cert_file', type=str, default=None, help='Path to file for cert file for TLS')
    parser.add_argument('--key_file', type=str, default=None, help='Path to file for key file for TLS')

    args = parser.parse_args()

    if args.cert_file is not None and args.key_file is not None:
        location = flight.Location.for_grpc_tls(args.host, args.port)
        certs = _read_cert_files(args.cert_file, args.key_file)

        server = ShootsServer(location,
                              bucket_dir=args.bucket_dir,
                              certs=certs)
    # Check if both cert_file and key_file are None
    elif args.cert_file is None and args.key_file is None:
        location = flight.Location.for_grpc_tcp(args.host, args.port)
        server = ShootsServer(location, bucket_dir=args.bucket_dir)
    # If one is None and the other is not, raise an error
    else:
        raise ValueError("Both cert_file and key_file must be provided, or neither should be.")

    server.serve()