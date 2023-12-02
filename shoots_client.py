from pydantic import BaseModel, ValidationError, validator
from pydantic_settings import BaseSettings
from typing import Optional
import pyarrow as pa
from pyarrow.flight import FlightDescriptor, FlightClient
import pandas as pd

class ClientConfig(BaseSettings):
    host: str
    port: int

class PutRequest(BaseModel):
    dataframe: pd.DataFrame
    name: str

    class Config:
        arbitrary_types_allowed = True

    @validator('dataframe')
    def validate_dataframe(cls, v):
        if not isinstance(v, pd.DataFrame):
            raise ValueError('table must be a pandas DataFrame')
        return v
    
class ShootsClient:
    def __init__(self, host: str, port: int):
        try:
            config = ClientConfig(host=host, port=port) #validate input
            self.client = FlightClient(f"grpc://{config.host}:{config.port}")
        except ValidationError as e:
            print(f"Configuration error: {e}")
            raise

    def put(self, name: str, dataframe: pd.DataFrame):
        try:
            PutRequest(dataframe=dataframe, name=name) #validate input
            descriptor = FlightDescriptor.for_path(name)
            table = pa.Table.from_pandas(dataframe)
            writer, _ = self.client.do_put(descriptor, table.schema)
            writer.write_table(table)
        except ValidationError as e:
            print(f"Validation error: {e}")

    def get():
        pass