from tls_test import TLSTest
from shoots_server import ShootsServer
from shoots_client import ShootsClient
from pyarrow.flight import Location, FlightClient, FlightUnavailableError

class JWTTest(TLSTest):
    port = 8083
    bucket_dir = "tls_buckets"
    def _set_up_server(self):
        location = Location.for_grpc_tls("localhost", self.port)
        server = ShootsServer(location,
                            bucket_dir=self.bucket_dir,
                            certs=(self.server_cert,self.server_key),
                            secret="test_secret")
        self.token = server.generate_admin_jwt()
        return server
    
    def _set_up_shoots_client(self):
        return ShootsClient("localhost", 
                            self.port, 
                            True,
                            self.root_cert)

    def _set_up_flight_client(self):
        url = f"grpc+tls://localhost:{self.port}"
        kwargs = {"tls_root_certs":self.root_cert}
        return FlightClient(url, **kwargs)

    server_cert = """-----BEGIN CERTIFICATE-----
MIIDrDCCApSgAwIBAgIUD+l0waPPxuBtzvr6Rw221CugWIwwDQYJKoZIhvcNAQEL
BQAwYDELMAkGA1UEBhMCdXMxCzAJBgNVBAgMAm1kMRIwEAYDVQQHDAlyb2Nrdmls
bGUxDTALBgNVBAoMBHNlbGYxDTALBgNVBAsMBHNlbGYxEjAQBgNVBAMMCWxvY2Fs
aG9zdDAeFw0yNDAxMjAyMzA0MjZaFw0yNTA2MDMyMzA0MjZaMGAxCzAJBgNVBAYT
AnVzMQswCQYDVQQIDAJtZDESMBAGA1UEBwwJcm9ja3ZpbGxlMQ0wCwYDVQQKDARz
ZWxmMQ0wCwYDVQQLDARzZWxmMRIwEAYDVQQDDAlsb2NhbGhvc3QwggEiMA0GCSqG
SIb3DQEBAQUAA4IBDwAwggEKAoIBAQC2zmFFr73t+Lob9TAbVB+D14PyugObA0KO
LI5Ta1KZKNBjJ3In5yCbP4BipggzQIi77wjoRnzVwT0VVCqmCqh2wxpdG6snsqcj
XbXDMfrf2e4ojoo2DY4mmk7XQWGJEU89Uoj+ha7+Nz+rgBfjR1jk2nCF5f4hjWGp
HFryCvxgNdxsqw06+ETXcmPQtaoa8fVqYKu9r06sGC8EgzAKTdsq4KHqQTskpmqN
CSeFMIXlt2eFhtfC7hOAOkbw+IKHFPOEXVm6FK+OYt95Dt+TIpCCxRnOM/e9KNGc
6QvRzmAA0Ohf68EaZIjFXBbZxS7f6VJ9RC1f5VSKvuTvQyrwu9otAgMBAAGjXjBc
MBoGA1UdEQQTMBGCCWxvY2FsaG9zdIcEfwAAATAdBgNVHQ4EFgQUJIdGG820rPoV
14bBGtdl9FChbL0wHwYDVR0jBBgwFoAUExYiJmioMTbQzqFIy6J1/WXF0EEwDQYJ
KoZIhvcNAQELBQADggEBAJpbaiIh+t9vjT45kE/PR9DQjfhMOjC49mWl1eFLfY+C
fCICXD2UBrI265gdGlLCRiVU2HUNFP6AxaLW87zzJ69yIafgYXkAo1JPxdpQbBNq
n8gOLDkBLIdVGVCqFwG3HHaxMMf291yIVVJ7lX0ZQjuUNX0iAuL7fP/sdM2X+mZ5
oFtluuCNQWOPCHcssa/CIN/IZH7+9NtXxjisxng1xyGEZHKMGB1Dn2okqpP3zDlq
gOtqomX0d7LCb0fSTsBoXbINexYGt3mUemiaC59J7jpJHjARPTCY4N5BdPeYWfyI
3+RZlIfHY2N/WCrxVZ5hnJnynYhOQU637mhR9MOg3RI=
-----END CERTIFICATE-----"""

    server_key = """-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC2zmFFr73t+Lob
9TAbVB+D14PyugObA0KOLI5Ta1KZKNBjJ3In5yCbP4BipggzQIi77wjoRnzVwT0V
VCqmCqh2wxpdG6snsqcjXbXDMfrf2e4ojoo2DY4mmk7XQWGJEU89Uoj+ha7+Nz+r
gBfjR1jk2nCF5f4hjWGpHFryCvxgNdxsqw06+ETXcmPQtaoa8fVqYKu9r06sGC8E
gzAKTdsq4KHqQTskpmqNCSeFMIXlt2eFhtfC7hOAOkbw+IKHFPOEXVm6FK+OYt95
Dt+TIpCCxRnOM/e9KNGc6QvRzmAA0Ohf68EaZIjFXBbZxS7f6VJ9RC1f5VSKvuTv
Qyrwu9otAgMBAAECggEAS0HDQdELu8A9vey+lUGYh8pdg7uYaGR7LdSh8y2nn23x
/B/tGwhHoCIQVIxNq+vRXq0VWapLEiSvZOx1ck3I9Lt1Swds+rbNn/2gKm+U/DrX
lNK5i28hKjs0YeGEqcz6diBYTY/zbqyIBa6CJwrkSbKzyCva5y0xG1GVDzf+lgxi
yL6+UJtJmv5bhWegEipe3SOlb5DsUKIQGwZbEYMMaEYfM/jbIlnSLWNh+EB3Ep2z
N76JmgbH7TXpq16J9aTaVsQfggHNkfxmdzDMhFZOKZZQdfOLW+5OaoN/LXzT3L8Q
NR2VfNEIARwlnmv7vmkYOtarVJnCrWjm9oETiFkg8QKBgQD2QmmCLy2ivdRzEP55
A4elaQvm6j8olHFip0zJ/wEToLTV6032GazHb+W8WhSGmD3RAtTGAPJg8tfC4ofs
oRU3UroVJpbwWhBnx1YfhbfFYjmTIkgo4Xk34l5Auk2DhIL+i7mOPkrKXJvPPO+v
peUGJM5tWcYzR92AmlhQWuYs7wKBgQC+CXMBlSuCRd8o2OMg5/MW0Cgl/uk5KyMq
q2mUKNiVfBqlh1GOVvM3yYx7Q64z4LD7whmiF3O6Qc0Gh70qCARdnbUfZh+pr7rf
9TYAVfVgvjOa5gdZqTquBrxYYqb+BaHb0nid3gzenKIBJUN3nPR7+n8HO4zwFKgK
Mk2NkCKiowKBgQCthjaaiWv61RCy1DK8SwR2RgMg/8bpbVBIV+MAIzQ7BG7onhod
ZBIfpnWXt1HnVbeZZAlSTLB+KCBpLv7getFcXmrJJJwPWSdeOVQyeiL4bzJqvylh
xR+JhXUs3xpnpiQ4AXULClHhLkiMS0AJ4Tf0kFL1MuE6mVU3nYrFdIfk+QKBgBmt
PyvMkvdkJ3XmcDHnBTJk57fSjIKb8IF4baiGKFVyUkthUESyPOShx+hzyZra1i7+
F+aN0qYs893ZA1clhCl+AJYAf3C2/MH76NL8yk3LBT/9qqqTsgkHgfVTRwr1idwg
wQlbklOVyFHtTgNgYqxJuVYp1q4trFLMXywseHGPAoGAHVl2Sz7JWBilMVtGURmM
X3Cq1nQxf820+PQO06ebLUAqgSo3lloVybNFzzRGTmbtHjR6HkVKAfAkf3rj02uc
AlQB2BliRZIVqE5J8ZhGA1VSSHDQcVnSEoaL6VSdKhuNvUMoJbDAIOc5/6AxDg5c
W6wn/4QNjcFCLK7IxkU+mi0=
-----END PRIVATE KEY-----"""

    root_cert = """-----BEGIN CERTIFICATE-----
MIIDozCCAougAwIBAgIUEz+VqfyowWmoV3qCaOpMmPSaV1IwDQYJKoZIhvcNAQEL
BQAwYDELMAkGA1UEBhMCdXMxCzAJBgNVBAgMAm1kMRIwEAYDVQQHDAlyb2Nrdmls
bGUxDTALBgNVBAoMBHNlbGYxDTALBgNVBAsMBHNlbGYxEjAQBgNVBAMMCWxvY2Fs
aG9zdDAgFw0yNDAxMjAyMjU3MTlaGA8yMDUxMDYwNzIyNTcxOVowYDELMAkGA1UE
BhMCdXMxCzAJBgNVBAgMAm1kMRIwEAYDVQQHDAlyb2NrdmlsbGUxDTALBgNVBAoM
BHNlbGYxDTALBgNVBAsMBHNlbGYxEjAQBgNVBAMMCWxvY2FsaG9zdDCCASIwDQYJ
KoZIhvcNAQEBBQADggEPADCCAQoCggEBAK4sJWgQNAVpEcAvQD3Je1KAnZIzhnZU
jnbgER8ruF/nssg8lChclTyTEJIScCVzNDcUiMqDqlqhKDrslzNcTrXWQfnpPlSP
RU8SXdKKOX1+CMZ2I+zATUo/i1XBcgLCp0n74VEpxgP51vZMkuaYnLqIBxJ5z+uF
sJsZi3BwzDB2CwSgCVY6XXZFrp2lLw15yXKX/wtJ/rZzzWDH30kGDGS5K8CkfRN9
E2zQteKl3bdgaVMyii9vr/KB74C+nk9126U3GALVOpn1pCbt/cWM0Z2MBzyr3/RS
DAyukghgz5bx/le2ThBo67c0nbr1PFZ+ZcmFAuiOa28QxrhIwTqnzH8CAwEAAaNT
MFEwHQYDVR0OBBYEFBMWIiZoqDE20M6hSMuidf1lxdBBMB8GA1UdIwQYMBaAFBMW
IiZoqDE20M6hSMuidf1lxdBBMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQEL
BQADggEBADSFiONPT/kKnqrGTMD5Ehvi6m/gyhRUKBeO8Zk4xRYl5mLp3Gq8YqOk
k2IHuFyRSWDaOt2SGyHyfi9kA9MC3eigGYB8jbRqJRPumTkGpbyr0Ao8cHt9Eghh
bZMxcQdpzbVv4q/zyD1jc/eE47yy8ddm1RIR3Xx5zKVuLHNvV9MXJs/bmWZDlH2G
szsMWLGz/HjezdNh5FMmyj60fEOQerjf+FZBZNYFk//siLKnnJJMVABNVXYk4i+7
lZNEMvLlQ/j48ORH46+4lutnDTdTGIiyHroaq2XFHqNay4jfrl7FZwwlJ18jfj+J
3FosYGMK6Jm+Zqrg4fK8yeu8ozGOhII=
-----END CERTIFICATE-----"""
