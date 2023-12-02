from shoots_client import ShootsClient
from shoots_client import PutMode
import pandas as pd
data = {"col1":[0,1],"col2":["zero","one"]}
df = pd.DataFrame(data)
client = ShootsClient("localhost", 8081)
client.put("foo",df,mode=PutMode.ERROR)

print(client.get('foo'))