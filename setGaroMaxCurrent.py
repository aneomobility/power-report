import json
import os
from dotenv import load_dotenv
import pandas as pd
import psycopg
import requests

load_dotenv()

DB_URL = os.getenv("DB_URL")


data = pd.read_csv("data/GARO/circuit_avg.csv")

circuits = data["circuit_id"].unique().tolist()

query = f"""
    SELECT "circuitId", "backplateName" FROM "PulseStructureChargingUnit" WHERE "circuitId" IN ({",".join(list(map(lambda x: f"'{x}'", circuits)))}) AND "isMaster" = True AND "provider" = 'GARO'
"""
db_data = []
with psycopg.connect(DB_URL) as conn:
    with conn.cursor() as cur:
        cur.execute(query)
        for record in cur:
            db_data.append([record[0], record[1]])

data["backplateName"] = data["circuit_id"].apply(
    lambda x: next(filter(lambda y: y[0] == x, db_data))[1]
)


for i, row in data.iterrows():
    chargerId = row["backplateName"]
    url = f"https://api.emabler.io/api/charger/changeConfig/{chargerId}?code=aN9ZvmRWruynYuBfkL2cE9JGHyGV4qV7XZAypyn2XJNMRuxc87Dksku6ifvVoBPitrvNRxcLWnju8HZ2DGsjEVyqUaXXAte23LKd&clientId=OHMIA"
    payload = json.dumps(
        {
            "configName": "GaroConnectionGroupMaxCurrent",
            "configValue": str(int(row["amps"])),
        }
    )
    headers = {
        "Content-Type": "text/plain",
        "Accept": "text/plain",
    }
    print(chargerId, payload)
    response = requests.request("POST", url, headers=headers, data=payload)
    print(response)
