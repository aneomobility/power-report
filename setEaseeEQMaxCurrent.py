import json
import math
import os

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()


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


equalizerId = "QH3WB2XD"

url = f"https://api.easee.com/api/equalizers/{equalizerId}/commands/configure_max_allocated_current"


maxKW = 14.90

maxW = maxKW * 1000

volt = 230

amps = maxW / (volt * math.sqrt(3))


headers = {"content-type": "application/*+json", "Authorization": f"Bearer {token}"}
payload = json.dumps({"maxCurrent": int(amps)})

response = requests.post(url, data=payload, headers=headers)
print(response.json())
