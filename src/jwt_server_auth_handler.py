from pyarrow.flight import ServerAuthHandler

class JWTServerAuthHandler(ServerAuthHandler):
    def __init__(self):
        super().__init__()

    def authenticate(self, outgoing, incoming):
        token = incoming.read()
        print(token)
    
    def is_valid(self, token):
        return b'noted'
        
