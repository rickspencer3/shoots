from base_test import ShootsTestBase
from shoots import ShootsServer, ShootsClient
from pyarrow.flight import Location, FlightClient, FlightUnavailableError

class TLSTest(ShootsTestBase):
    port = 8083
    bucket_dir = "tls_buckets"
    def _set_up_server(self):
        location = Location.for_grpc_tls("0.0.0.0", self.port)
        return ShootsServer(location,
                            bucket_dir=self.bucket_dir,
                            certs=(self.server_cert,self.server_key))
    
    def _set_up_shoots_client(self):
        return ShootsClient("localhost", 
                            self.port, 
                            True,
                            self.root_cert)

    def _set_up_flight_client(self):
        url = f"grpc+tls://localhost:{self.port}"
        kwargs = {"tls_root_certs":self.root_cert}
        return FlightClient(url, **kwargs)

    def test_use_address_not_in_cert(self):
        with self.assertRaises(FlightUnavailableError):
            ShootsClient("0.0.0.0", 
            self.port, 
            True,
            self.root_cert).ping()

    def test_use_alternate_ip_address(self):
        c = ShootsClient("127.0.0.1", 
            self.port, 
            True,
            self.root_cert)
        result = c.ping()
        self.assertEqual(result,"pong")

    server_cert = """-----BEGIN CERTIFICATE-----
MIIDYzCCAkugAwIBAgIUX+8kNKegEIrbqmNgozKwOJAG0Z8wDQYJKoZIhvcNAQEL
BQAwVTELMAkGA1UEBhMCVVMxCzAJBgNVBAgMAk1EMRIwEAYDVQQHDAlST0NLVklM
TEUxEDAOBgNVBAoMB2luZmVyZGIxEzARBgNVBAMMCmluZmVyZGJfY2EwHhcNMjUw
MjA5MTQyOTI3WhcNMjYwMjA5MTQyOTI3WjBUMQswCQYDVQQGEwJVUzELMAkGA1UE
CAwCTUQxEjAQBgNVBAcMCVJPQ0tWSUxMRTEQMA4GA1UECgwHaW5mZXJkYjESMBAG
A1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA
1Z2GRPh+zI8KVMkQEOdmFC9V0WHnAJZZVYBB1RTlBSHPKStD3AFGulL0C02j+Xti
oAA+kG6voWVoJDwSdCToSUXhsQKxmHPgpP/+2srXnhZTJIgyqQVCa2PmFGtOzvrE
IvIRtPIlH9T3jENeOCcUbPb8qMx5WqlZyhVNKxLD7p0QwZSwy6RmWTed/RUQZYdq
UZbms6fEcE8Y/kY3MawEyemJNmpPao2Fmt2IaLnTumgsV26RZql6099lWZGYrF9p
Xe3OvAmZNbXQ/NhvgJ8mAmwY6vN/wgX8O5CwIg3y4cY01GN6rLDZOU7hkCcfwQIQ
fVtMbeL2I2GdqPEXGOqlvwIDAQABoywwKjAMBgNVHRMBAf8EAjAAMBoGA1UdEQQT
MBGHBH8AAAGCCWxvY2FsaG9zdDANBgkqhkiG9w0BAQsFAAOCAQEAG9FaNORGmE4p
2fmxPT6hSg5zlS7cO+XWVD53X13iqy0wR3vn+F9ka1zPwE53Nz3MV2IRpEfHy+Zs
dzFfZo6W2dXzxZgRNxfDXZUHbdIfoAP4e8B6w/cpWS3XNDcvahpuLHwH8Xrfn9D+
NFMyMXC+SiEM2lUMu/OQTS8HKTXdSuTLuX44Z92aquCT2rmPg1tPc+gcZXqK2Qt8
qeYH0t+zJbIEofzJCGjC1SWD4uhJUJLqHt571+rgafZQQU7yuVHMmvz5hmprjpSU
I+vNaiLJW0KnZeR/oQetGLx87Xnf5JwTQoSxrHtcARcBCyNO1g7ifZ0DGqDducPG
3bwodtnyFA==
-----END CERTIFICATE-----"""

    server_key = """-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEA1Z2GRPh+zI8KVMkQEOdmFC9V0WHnAJZZVYBB1RTlBSHPKStD
3AFGulL0C02j+XtioAA+kG6voWVoJDwSdCToSUXhsQKxmHPgpP/+2srXnhZTJIgy
qQVCa2PmFGtOzvrEIvIRtPIlH9T3jENeOCcUbPb8qMx5WqlZyhVNKxLD7p0QwZSw
y6RmWTed/RUQZYdqUZbms6fEcE8Y/kY3MawEyemJNmpPao2Fmt2IaLnTumgsV26R
Zql6099lWZGYrF9pXe3OvAmZNbXQ/NhvgJ8mAmwY6vN/wgX8O5CwIg3y4cY01GN6
rLDZOU7hkCcfwQIQfVtMbeL2I2GdqPEXGOqlvwIDAQABAoIBACyNTJYw750xThnN
z3R/FnqqPq9LSHcGZH5hIBnVYEiYOxEejWoiuAIfT/fVixf65GBzfJj6BNZYBIbe
Iokns2yozv/wjGK79EPlgl1Whe7aQB/z6gD3cFleffuPP+IKSgLx0sCW6ig/7htK
Z6m1eNybDKoV709515i+pZgQs12d8ahTq06xYfV+kZW9rUDvW3wMOwYFd85B6o70
bmwg6kBbaSUctp3+fEZnLX30eSah4BvVMUYtOW15jnzUlERDt0xiwQB2pJjyMKpF
EvCn1Zbdg7qxLaUgjB+5fs8W5r1UFvqAERhFpsXBFU3natL2MQm+ViX5wojHMX+v
iT3OquECgYEA/TxyMVQebxP0IHDabu2N7UU92OqDyTQeyAaA0rTVdyFFaVkvku5S
Rzi4tNqJwX5cGbC2o7LQBcjs67Xxt3xPZgMUvspAb0YvPgS26IlN69ZCOuCbkI0S
FR61b7NnLflmkNvCECKLFjmlPf/JKmkDx0TzizlFIHGisjC9kJnXuiECgYEA1/Jg
Q6hkwZXrnvRxVrywnVudesjvR9oeVvavWb7OLJ4hiP2fzdfmSHPQ0/I9Yr1yWTWn
b3rFsilpFn0pslSJuT3V3JglKRTw9C8T9DfyVMhwIM5dbzl7SptB7xIudL4RSmDg
WpXbpkKwEkDzeod/5sMdtfscOoDlUZ7uvmftI98CgYBX4hpfNCo7slkOyRuFU7vl
lBoapYiG0ye7k0Yx2cAbT9ie7uyruTmkrfKsEQutswSThdhchznaSBiw1LvGScXk
ST1x2Q7zjw/mHgy8NqpsJjdl530VdV/JJJci5MfyFJObwihfIR3T4L2P/qz5ouhE
x5EdyApNcCJeZLvk6v8LAQKBgCKGf9mRXLqOq6M4Vb6WYG5oLV9qLMeCGgOxYQuq
M/ByP85VLm2MrSa8TZD1U9crjtKwf1qVeHIpqMGNtVLrrTFOr5ibQqW00WlY9YnA
QCBKA5NbKxkTSaD9/aapc3/6u2z23CffecS1OM1SURsv+sT8QQ3NXhAEd6V7EZSj
rQhlAoGBAIXGg/+JrDqCeQYIiU+HiGbo5QU9wADc6LQt6QZLwuow/w5G6ThNfEgp
83IqO0qx5iPxdztiWQVfbtkl7JbWk7cgBv2a9ebi0UCxlmZtl2C6qNTh8j6CwMHz
l043Dg83JU7Pk1jmr137HDHJOH3YiENFF4FCarpIR02RBxha+WvZ
-----END RSA PRIVATE KEY-----"""

    root_cert = """-----BEGIN CERTIFICATE-----
MIIDSzCCAjOgAwIBAgIUX3+2Xld7sO9u6dUtw2Xi/v2CHh8wDQYJKoZIhvcNAQEL
BQAwVTELMAkGA1UEBhMCVVMxCzAJBgNVBAgMAk1EMRIwEAYDVQQHDAlST0NLVklM
TEUxEDAOBgNVBAoMB2luZmVyZGIxEzARBgNVBAMMCmluZmVyZGJfY2EwHhcNMjUw
MjA5MTQyOTI3WhcNMjYwMjA5MTQyOTI3WjBVMQswCQYDVQQGEwJVUzELMAkGA1UE
CAwCTUQxEjAQBgNVBAcMCVJPQ0tWSUxMRTEQMA4GA1UECgwHaW5mZXJkYjETMBEG
A1UEAwwKaW5mZXJkYl9jYTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB
ANgtR2wdouNrRhrhB6m31SYl9t+k4C+XQZPXz3G0+Cr8PXid8mIb9T40+qDxMCTX
SKfmmbD1BNhFaNUmmmWrYQyksbR3zSKS7chlgBbggM6zm/2Dj7zNdTIMkVz2s6Iq
0gf25ADSu3HqTVbSr0FWMVuw2EoiKCWMrHQcdpHeAJT6n0M0NQCkdKQ7hJ1O+NBx
uIdYxLMiwOzCtygZqDr1Dbzy2PjOfX44RipemEsC07QH80lbn2RAj0kXicZpE14O
YlAyWFHDbGp8nGH7gvQ7e9UjMrP4WNCX3hGZrh1vEsG977IxfmBFZ1wwJeqDS4XL
ZuRSzhPNwZu2mL1phFp+bocCAwEAAaMTMBEwDwYDVR0TAQH/BAUwAwEB/zANBgkq
hkiG9w0BAQsFAAOCAQEAayNNosR26LasZboTbgliJlkxLZhWyIcIKnJHJB5jddgr
rbE30AQ5KZ6RBdl2gJs1X1SK2HHLQ1QPepi2Bku7dUrUzh7wHbUPK8+4k4h5NGX3
vsjyyVpFWfGtCSZeElAtnheq56bE3fpOOlLxgM4RTdzuNdE7+s5qvgRNbSC3HOQb
BrecC8foA1w9FckcDr8buAW5z9UKCfrta08/bIXZx0laQVqgByqpwsM6XgjPHkAk
3bs1bkaajrK7KWiSKvAZNUcWU0JGz69BaHaTw0HDHAvDua8aDykZhtvFax742zBC
2dtRDiphMkVH3PDhVAxA4EQUpRyjugAkWxLPFiJhZQ==
-----END CERTIFICATE-----"""
