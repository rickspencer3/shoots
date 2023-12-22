from pydantic import BaseModel, ValidationError, validator
from pydantic_settings import BaseSettings
from typing import Optional
import pyarrow as pa
from pyarrow.flight import FlightDescriptor, FlightClient, Ticket, Action, FlightError
import pandas as pd
import json
from enum import Enum

class PutMode(Enum):
    REPLACE = "replace"
    APPEND = "append"
    ERROR = "error"

class ClientConfig(BaseSettings):
    host: str
    port: int

class DeleteRequest(BaseModel):
    name: str

    @validator('name')
    def validate_name(cls, v):
        if not v or not isinstance(v, str):
            raise ValueError('name must be a non-empty string')
        return v
    
class PutRequest(BaseModel):
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

class GetRequest(BaseModel):
    name: str
    sql: Optional[str] = None
    bucket: Optional[str] = None

    @validator('name')
    def validate_name(cls, v):
        if not v or not isinstance(v, str):
            raise ValueError('name must be a non-empty string')
        return v

class ShootsClient:
    def __init__(self, host: str, port: int):
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

    def list(self):
        action = Action("list",json.dumps({}).encode())
        result = self.client.do_action(action)
        list_string = None
        for r in result:
            list_string = r.body.to_pybytes().decode()
        return json.loads(list_string)
        

    def delete(self, name: str, bucket: Optional[str] = None):
        action_description = json.dumps({"name":name, "bucket":bucket}).encode()
        action = Action("delete",action_description)
        result = self.client.do_action(action)

        msg = ""
        for r in result:
            msg += r.body.to_pybytes().decode() + "\n"
        
        msg = msg.rstrip("\n")
        return msg