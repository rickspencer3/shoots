from pyarrow.flight import ServerAuthHandler, FlightUnauthenticatedError
import jwt

class JWTServerAuthHandler(ServerAuthHandler):
    def __init__(self, secret):
        self.secret = secret
        super().__init__()

    def authenticate(self, outgoing, incoming):
        token = incoming.read()
        try:
            permissions = jwt.decode(token, self.secret , algorithms=["HS256"])
            if permissions["type"] != 'admin':
                raise FlightUnauthenticatedError()
        except Exception as e:
            raise e

    def is_valid(self, token):
        return b'noted'
        
