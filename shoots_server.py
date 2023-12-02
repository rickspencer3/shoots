import os
import pyarrow as pa
from pyarrow import flight
import pyarrow.parquet as pq
from datafusion import SessionContext
import json

class ShootsServer(flight.FlightServerBase):
    def do_get(self, context, ticket):
        try:
            ticket_obj = json.loads(ticket.ticket.decode())
            name = ticket_obj["name"]
            file_name = f"{name}.parquet"

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
            print(e)
    
    def do_put(self, context, descriptor, reader, writer):
        command = json.loads(descriptor.command.decode())

        # Extract name and mode from the command
        name = command["name"]
        mode = command["mode"]

        data_table = reader.read_all()
        file_path = f"{name}.parquet"
        
        if os.path.exists(file_path):
            if mode == "append":
                existing_table = pq.read_table(file_path)
                data_table = pa.concat_tables([data_table, existing_table])
                pq.write_table(data_table, file_path)
            
            if(mode != "ignore"):
                pq.write_table(data_table, file_path)
        else:
            pq.write_table(data_table, file_path)

    def do_action(self, context, action):
        action, data = action.type, action.body.to_pybytes().decode()
        data = json.loads(data)
        if action == "delete":
            return self._delete(data)
        if action == "list":
            pass

    def _delete(self, data):
        file_path = f"{data['name']}.parquet"
            
        msg = f"{data['name']} deleted succesfully"
        success = True
        try:
            os.remove(file_path)
        except FileNotFoundError:
            msg = (f"{data} does not exist.")
            success = False
        except PermissionError:
            msg = f"Permission denied: unable to delete {data}."
            success = False
        except OSError as e:
            msg = f"Error deleting file {file_path}: {e}"
            success = False
            
        bytes = json.dumps({"success":success, "message":msg}).encode()
        result = flight.Result(bytes)
        return [result]
        
def run_flight_server():
    location = flight.Location.for_grpc_tcp("localhost", 8081)
    server = ShootsServer(location)

    print("Starting Flight server on localhost:8081")
    server.serve()

if __name__ == "__main__":
    run_flight_server()