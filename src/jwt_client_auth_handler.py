from pyarrow.flight import ClientAuthHandler

class JWTClientAuthHandler(ClientAuthHandler):
    def __init__(self, token):
        self.token = token
    
    def authenticate(self, outgoing, incoming):
        outgoing.write(self.token)
    
    def get_token(self):
        return self.token