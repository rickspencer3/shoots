from pyarrow.flight import ServerAuthHandler, FlightUnauthenticatedError, ServerMiddleware, FlightMethod
import jwt
class JWTMiddleware(ServerMiddleware):
    """Middleware that implements username-password authentication."""
    def call_completed(self, exception):
        pass

    def sending_headers(self):
        """Return the authentication token to the client."""
        return None
    
    def start_call(self, info, headers):
        """Ensure JWT is provided for all calls"""
        if "auth-token-bin" not in headers and info.method != FlightMethod.HANDSHAKE:
            raise FlightUnauthenticatedError("No authentication header provided by client")

class JWTServerAuthHandler(ServerAuthHandler):
    def __init__(self, secret):
        self.secret = secret
        super().__init__()

    def authenticate(self, outgoing, incoming):
        token = incoming.read()
        try:
            permissions = jwt.decode(token, self.secret , algorithms=["HS256"])
            if permissions.get("type") != 'admin':
                raise FlightUnauthenticatedError("token has incorrect permissions")
        except Exception as e:
            raise e

    def is_valid(self, token):
        return b''
        
