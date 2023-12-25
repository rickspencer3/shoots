import os
import pyarrow as pa
from pyarrow import flight
import pyarrow.parquet as pq
from datafusion import SessionContext
import json
import shutil
import threading
import argparse

class ShootsServer(flight.FlightServerBase):
    """
    A FlightServer for managing and serving pandas datasets.

    Attributes:
        location (pyarrow.flight.Location): The server location.
        bucket_dir (str): Directory path for storing parquet datasets.

    Note:
        You most likely don't want to use the server directly, accept for starting it up. It is easiest to interact with the server via ShootsClient.
    """

    def __init__(self, location, bucket_dir, *args, **kwargs):
        """
        Initializes the ShootsServer.

        Args:
            location (pyarrow.flight.Location): The location where the server will run.
            bucket_dir (str): Directory path where the parquet files will be stored.
        """
        self.location = location
        self.bucket_dir = bucket_dir
        super(ShootsServer, self).__init__(location, *args, **kwargs)

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
            ticket_obj = json.loads(ticket.ticket.decode())
            name = ticket_obj["name"]
            bucket = ticket_obj["bucket"]
            file_name = self._create_file_path(name, bucket)
            if "sql" in ticket_obj:
                sql_query = ticket_obj["sql"]
                ctx = SessionContext()
                ctx.register_parquet(name, file_name)
                
                result = ctx.sql(sql_query)
                table = result.to_arrow_table()
                
                return flight.RecordBatchStream(table)
            
            else:
                table = pq.read_table(file_name)
                return flight.RecordBatchStream(table)
            
        except Exception as e:
            raise flight.FlightServerError(extra_info=str(e))
    
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
        command = json.loads(descriptor.command.decode())

        # Extract name and mode from the command
        name = command["name"]
        mode = command["mode"]
        bucket = command["bucket"]

        data_table = reader.read_all()
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

        data_obj = json.loads(criteria.decode())
        bucket = data_obj["bucket"]
        regex = data_obj["regex"]
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
            # delete example with optional bucket name
            action_description = json.dumps({"name":"my_dataframe", "bucket":"my_bucket"}).encode()
            action = Action("delete",action_description)
            self.client.do_action(action)

            # buckets example
            bytes = json.dumps({}).encode()
            action = Action("buckets",bytes)
            result = self.client.do_action(action)
            for r in result:
                print(r.body.to_pybytes().decode())

            # delete_bucket example
            action_obj = {"name":"my_bucket, "mode":"error"}
            bytes = json.dumps(action_obj).encode()
            action = Action("delete_bucket",bytes)
            self.client.do_action(action)

            # shutdown example
            bytes = json.dumps({}).encode()
            action = Action("shutdown", bytes)
            self.client.do_action(action)
            ```
        """

        action, data = action.type, action.body.to_pybytes().decode()
        if data:
            data = json.loads(data)
        if action == "delete":
            return self._delete(data)
        if action == "buckets":
            return self._buckets()
        if action == "delete_bucket":
            return self._delete_bucket(data)
        if action == "shutdown":
            return self.shutdown()

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
            ("shutdown", "Shutdown the server")
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
        
    def _delete_bucket(self, data):
        bucket = data["name"]
        mode = data["mode"]
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
        result_data = {"message":f"bucket {bucket} deleted"}
        return self._flight_result_from_dict(result_data)

    def _buckets(self):
        entries = os.listdir(self.bucket_dir)
        buckets = [entry for entry in entries if os.path.isdir(os.path.join(self.bucket_dir, entry))]
        return self._list_to_flight_result(buckets)

    def _list_to_flight_result(self, buckets):
        bytes = json.dumps(buckets).encode()
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
    
    def _delete(self, data):
        name = data["name"]
        bucket = data["bucket"]
        file_path = self._create_file_path(name, bucket)

        try:
            os.remove(file_path)
        except FileNotFoundError:
            raise flight.FlightServerError(f"Dataset {name} not found",
                                extra_info="No Such Dataset")
        except PermissionError:
            raise flight.FlightUnauthorizedError(f"Insufficent permisions to delete {name}")
        except OSError as e:
            raise flight.FlightServerError(f"Error encountered deleting {name}",
                                           extra_info=str(e))
        
        result_data = {"success":True, "message":f"deleted {name}"}
        return self._flight_result_from_dict(result_data)

    def _flight_result_from_dict(self, result_data):
        bytes = json.dumps(result_data).encode()
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
        
        print("Shutting down ...")
        return self._list_to_flight_result(["shutdown command received"])

    def serve(self):
        """
        Serve until shutdown is called.

        """
        print(f"Starting Flight server on {self.location.uri.decode()}")
        
        super(ShootsServer, self).serve()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Starts the Shoots Flight Server.')
    parser.add_argument('--port', type=int, default=8081, help='Port number to run the Flight server on.')
    parser.add_argument('--bucket_dir', type=str, default='buckets', help='Path to the bucket directory.')

    args = parser.parse_args()
    location = flight.Location.for_grpc_tcp("localhost", args.port)
    server = ShootsServer(location, bucket_dir=args.bucket_dir)
    server.serve()