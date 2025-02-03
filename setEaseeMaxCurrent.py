import json
import os

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

data = pd.read_csv("data/EASEE/circuit_avg.csv")

# for each row in the data do a request to api

EASEE_PASSWORD = os.getenv("EASEE_PASSWORD")


def easee_login():
    url = "https://api.easee.com/api/accounts/login"

    payload = json.dumps(
        {"userName": "produkt.mobility@aneo.com", "password": EASEE_PASSWORD}
    )
    headers = {
        "Content-Type": "application/json",
    }

    response = requests.post(url, headers=headers, data=payload)
    return response.json()["accessToken"]


token = easee_login()

for index, row in data.iterrows():
    print(row["site_id"], row["site_key"], row["circuit_id"], row["amps"])
    url = f"https://api.easee.com/api/sites/{row['site_id']}/circuits/{row['circuit_id']}/settings"

    headers = {"content-type": "application/*+json", "Authorization": f"Bearer {token}"}
    payload = json.dumps(
        {
            "maxCircuitCurrentP1": row["amps"],
            "maxCircuitCurrentP2": row["amps"],
            "maxCircuitCurrentP3": row["amps"],
        }
    )

    response = requests.post(url, data=payload, headers=headers)
    print(response)
