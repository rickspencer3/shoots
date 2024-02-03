from base_test import BaseTest
from shoots_server import ShootsServer
from shoots_client import ShootsClient
from pyarrow.flight import Location, FlightClient

class InsecureTest(BaseTest):
    port = 8082
    bucket_dir = "insecure_buckets"
    def _set_up_server(self):
        location = Location.for_grpc_tcp("localhost", self.port)
        return ShootsServer(location, bucket_dir=self.bucket_dir)
    
    def _set_up_shoots_client(self):
        return ShootsClient("localhost", self.port)

    def _set_up_flight_client(self):
        url = f"grpc://localhost:{self.port}"
        return FlightClient(url)

    