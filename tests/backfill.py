from mastodon import Mastodon
import pandas as pd
from bs4 import BeautifulSoup
import time
from inferdb.inferdb_client import InferDBClient, PutMode

mastodon = Mastodon(
    client_id='	ex-wzjfmskieejKUHPldp43rNqMSQUavyolZTHjZhCI',
    client_secret='	vPkK6qWyUIC6pmpGuxFvAzNyd2MuSbTVqSzlB1O5D_0',
    access_token='E8dCFygoicLt9YKQXIGgQewO4u5pcO4lDlTGp1WSPQ4',
    api_base_url='https://social.lol'  # Replace with the base URL of the instance you're using
)

inferdb_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NTcwMTU0NDcsImlhdCI6MTcyNTQ3OTQ0Nywic2VydmVyIjoiZ3JwYyt0bHM6Ly9sb2NhbGhvc3Q6ODA4NSIsInR5cGUiOiJhZG1pbiJ9.YOvLLtitxM5o3GWe_pYsfuItyA8ZkUdPDwCj8zQ-nnc"
client = InferDBClient("localhost", 
                        8085,
                        certs_dir="../inferdb/inferdb/certs",
                        token=inferdb_token)

old_max = 113102261066116194
max_id = None
window_overlapped = False
while True:
    if(window_overlapped):
        print("Backfill complete, quitting ...")
        break
    toots = mastodon.timeline_public(max_id=max_id)
    toots_data = []
    for toot in toots:
        id = toot["id"]
        created_at = toot["created_at"]
        account = toot["account"]
        content = BeautifulSoup(toot["content"], "html.parser").get_text()
        if id > old_max:
            toots_data.append({
                "id": id,
                "created_at": created_at,
                "account_name": account["display_name"],
                "account_acct": account["acct"],
                "content": content,
                "language":toot["language"],
                "replies_count":toot["replies_count"]
            })
        else:
            window_overlapped = True

    if (len(toots_data) > 0):
        df = pd.DataFrame(toots_data)
        df = df.sort_values(by='id')        
        print(df)
        try:
            result = client.put("mastodon",
                                df,
                                mode=PutMode.APPEND)
            max_id = df['id'].min()
            print(df['created_at'].min(), df["content"].count())
        except Exception as e:
            print(f"Failed to write dataframe")
            print(e)
    time.sleep(1)