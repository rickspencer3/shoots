from pyarrow.flight import ServerAuthHandler, FlightUnauthenticatedError, ServerMiddleware, FlightMethod
import jwt

"""
I think this how it works:
1. The ServerMiddleware is provided to the FlightServer. The middleware has functions that get called by the FlightServer
at specific points in the call process. We only care about validating each call, so we want to check at the start of each call.
2. The ClientMiddleware is provided to the client, and the client calls authenticate on it for each call.
3. At the start of each call the ServerMiddle ware start_call() function is called. The client could be instigating a handshake,
but since the 
Unless is it a handshake, we make certaint that the client is passing a token. Otherwise, the client could simply
not provide any middleware, and the server would server wihtout authentication,
4. For each call, the ClientMiddleware calls it's authenticate method, which simply sends the token to the 
ServerMiddleware's authentication method. If there is a problem decoding the token or if the 
permissions encoded in the token are incorrect, an exception is thrown before the call to the server is made.
"""

class JWTMiddleware(ServerMiddleware):
    """Middleware that implements username-password authentication."""
    def call_completed(self, exception):
        pass

    def sending_headers(self):
        """Return the authentication token to the client."""
        return None
    
    def start_call(self, info, headers):
        """Ensure JWT is provided for all calls"""
        if info.method == FlightMethod.HANDSHAKE:
            # the client is setting up an initial handshake
            # because all of the permissions are embedded in the jwt there is nothing to do
            return
        
        if "auth-token-bin" not in headers:
            # This happens for a normal call, but the client is going to make
            # the call without calling JWTServerAuthHandler.authenticate(),
            # typically because it is not usig JWTClientMiddleware.
            # the token will decoded and verified in the JWTServerAuthHandler.authenticate method.
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
        
