import os
import pyarrow as pa
from pyarrow import flight
import pyarrow.parquet as pq
from datafusion import SessionContext
import json
import shutil
import threading

class ShootsServer(flight.FlightServerBase):
    bucket_dir = "buckets"
    def __init__(self, location, *args, **kwargs):
        self._location = location
        super(ShootsServer, self).__init__(location, *args, **kwargs)

    def do_get(self, context, ticket):
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
        action, data = action.type, action.body.to_pybytes().decode()
        data = json.loads(data)
        if action == "delete":
            return self._delete(data)
        if action == "list":
            return self._list_parquet_files(data)
        if action == "buckets":
            return self._buckets()
        if action == "delete_bucket":
            return self._delete_bucket(data)
        if action == "shutdown":
            shutdown_thread = threading.Thread(target=self.shutdown)
            shutdown_thread.start()
            print("Shutting down ...")
            return self._list_to_flight_result(["shutdown command set"])

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
        
    def run(self):
        print("Starting Flight server on localhost:8081")
        self.serve()

if __name__ == "__main__":
    location = flight.Location.for_grpc_tcp("localhost", 8081)
    server = ShootsServer(location)
    server.run()