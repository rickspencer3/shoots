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
            sql_query = ticket_obj["sql"]
            table_name = ticket_obj["table"]
            
            ctx = SessionContext()
            ctx.register_parquet(table_name, f"{table_name}.parquet")
            
            result = ctx.sql(sql_query)
            table = result.to_arrow_table()
            
            return flight.RecordBatchStream(table)
        except Exception as e:
            print(e)
    
    def do_put(self, context, descriptor, reader, writer):
        table_name = descriptor.path[0].decode('utf-8')
        data_table = reader.read_all()
        file_path = f"{table_name}.parquet"
        
        if os.path.exists(file_path):
            try:
                existing_table = pq.read_table(file_path)
                data_table = pa.concat_tables([data_table, existing_table])
            except Exception as e:
                print(e)

        try:
            pq.write_table(data_table, file_path)
        except Exception as e:
            print(e)
        
    def do_action(self, context, action):
        bytes = json.dumps({"action":action.type}).encode()
        result = flight.Result(bytes)
        return [result]

def run_flight_server():
    location = flight.Location.for_grpc_tcp("localhost", 8081)
    server = ShootsServer(location)

    print("Starting Flight server on localhost:8081")
    server.serve()

if __name__ == "__main__":
    run_flight_server()