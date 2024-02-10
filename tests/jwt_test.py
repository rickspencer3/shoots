from .tls_test import TLSTest
from shoots import ShootsServer, ShootsClient, JWTClientAuthHandler
from pyarrow.flight import Location, FlightClient, FlightServerError, FlightUnauthenticatedError
import datetime
import jwt

class JWTTest(TLSTest):
    port = 8084
    bucket_dir = "jwt_buckets"
    def _set_up_server(self):
        self.location = Location.for_grpc_tls("localhost", self.port)
        server = ShootsServer(self.location,
                            bucket_dir=self.bucket_dir,
                            certs=(self.server_cert,self.server_key),
                            secret="test_secret")
        self.token = server.generate_admin_jwt()
        return server
    
    def _set_up_shoots_client(self):
        return ShootsClient("localhost", 
                            self.port, 
                            True,
                            self.root_cert,
                            token=self.token)

    def _set_up_flight_client(self):
        url = f"grpc+tls://localhost:{self.port}"
        kwargs = {"tls_root_certs":self.root_cert}
        client = FlightClient(url, **kwargs)
        auth_handler = JWTClientAuthHandler(token=self.token)
        client.authenticate(auth_handler)
        return client
    
    def test_garbage_token_rejected(self):
        with self.assertRaises(FlightServerError):
            bad_client = ShootsClient("localhost", 
                                self.port, 
                                True,
                                self.root_cert,
                                token="garbage")
            bad_client.ping()

    def test_no_token(self):
        with self.assertRaises(Exception):
            bad_client = ShootsClient("localhost", 
                                self.port, 
                                True,
                                self.root_cert)
            bad_client.ping()

    def test_incorrect_permissions_type(self):
            payload = {
            'exp': datetime.datetime.utcnow() + datetime.timedelta(days=365),
            'iat': datetime.datetime.utcnow(),
            'server': self.location.uri.decode(),
            'type':'user'}

            naughty_token = jwt.encode(payload, "test_secret", algorithm='HS256')    

            with self.assertRaises(FlightUnauthenticatedError):
                naughty_client = ShootsClient("localhost", 
                                    self.port, 
                                    True,
                                    self.root_cert,
                                    token=naughty_token)
            
                naughty_client.ping()