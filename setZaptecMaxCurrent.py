import json
import os
from dotenv import load_dotenv
import pandas as pd
import requests

load_dotenv()

data = pd.read_csv("data/ZAPTEC/circuit_avg.csv")

# for each row in the data do a request to api

ZAPTEC_PASSWORD = os.getenv("ZAPTEC_PASSWORD")


import requests


def token():
    url = "https://api.zaptec.com/oauth/token"

    payload = f"grant_type=password&username=lars.skjelbek%40aneo.com&password={ZAPTEC_PASSWORD}"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    return response.json()["access_token"]


access_token = token()

grouped_data = (
    data.groupby(
        [
            "site_id",
        ]
    )
    .sum(["avg_value_kWh", "amps"])
    .reset_index()
)
for index, row in grouped_data.iterrows():
    print(row["site_id"], row["amps"])
    installation_id = row["site_id"]
    url = f"https://api.zaptec.com/api/installation/{installation_id}/update"

    headers = {
        "accept": "*/*",
        "content-type": "application/*+json",
        "Authorization": f"Bearer {access_token}",
    }
    payload = json.dumps(
        {
            "availableCurrent": row["amps"],
        }
    )

    response = requests.post(url, data=payload, headers=headers)
    print(response)
