from datetime import datetime, timedelta
import os
import numpy as np
import pandas as pd
from azure.data.tables import TableClient
from azure.core.credentials import AzureNamedKeyCredential
from tqdm import tqdm
import concurrent.futures
import psycopg
from dataclasses import dataclass
from typing import List
from dotenv import load_dotenv
import requests


# Load environment variables
load_dotenv()


# Constants
DB_URL = os.getenv("DB_URL")
AZ_ACCOUNT = os.getenv("AZ_ACCOUNT")
AZ_KEY = os.getenv("AZ_KEY")
TABLE_ENDPOINT = "https://pulsehubstoraged998526f.table.core.windows.net"
OBS_IDS = [
    {"provider": "EASEE", "obs": "120", "table": "formattedobservations"},
    # {"provider": "ZAPTEC", "obs": "513", "table": "formattedobservations"},
    # {"provider": "GARO", "obs": "-36", "table": "emablerlog"},
]
DAYS_BACK_IN_TIME = 60


@dataclass
class ChargingUnit:
    site_id: str
    site_key: str
    name: str
    site_power: float
    circuit_id: str
    provider: str
    charger_id: str
    energy_area: str
    meter_nr: str


@dataclass
class ObsData:
    charger_id: str
    timestamp: str
    value: str
    observation_id: str


@dataclass
class ChargingUnitComplete:
    site_id: str
    site_key: str
    name: str
    provider: str
    energy_area: str
    spot_price: float
    site_power: float
    is_holiday: bool
    circuit_id: str
    timestamp: str
    charger_id: str
    value_kWh: str
    meter_nr: str


def get_charging_unit_data() -> List[ChargingUnit]:
    query = f"""
        WITH raw AS (
            SELECT
                "PulseStructureSite".id AS "siteId",
                "PulseStructureSite"."siteKey",
                "PulseStructureSite"."name",
                REGEXP_REPLACE("PulseStructureSite"."netType",'[^0-9]','','g')::INTEGER * "PulseStructureSite"."fuseRating" * SQRT(3) / 1000 AS "sitePower(kW)",
		        "PulseStructureCircuit".id AS "circuitId",
                "PulseStructureChargingUnit"."chargerId",
                "PulseStructureChargingUnit"."provider",
		        "PulseStructureSite"."salesforceId"
            FROM
                "PulseStructureSite"
            LEFT JOIN "PulseStructureCircuit" ON "PulseStructureSite".id = "PulseStructureCircuit"."siteId"
            LEFT JOIN "PulseStructureChargingUnit" ON "PulseStructureCircuit".id = "PulseStructureChargingUnit"."circuitId"
        WHERE
            "PulseStructureChargingUnit"."SF_active" = TRUE
            AND "PulseStructureSite"."salesforceId" IS NOT NULL
        ),
        counter AS (
            SELECT "siteKey", COUNT("chargerId") as "count" FROM raw GROUP BY "siteKey"
        )
        SELECT * FROM raw WHERE "siteKey" IN (SELECT "siteKey" FROM counter WHERE "count" <= 10) AND "sitePower(kW)" IS NOT NULL and "siteKey" = 'LN74-D222'
    """
    data = []

    sf_data = pd.read_csv("SF_energy_area.csv")
    print("Fetching data from database")
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            for record in tqdm(cur):
                sf = sf_data[sf_data["Id"] == record[7]]
                energy_area = sf.values[0][2]
                meter_nr = sf.values[0][3]
                data.append(
                    ChargingUnit(
                        site_id=record[0],
                        site_key=record[1],
                        name=record[2],
                        site_power=record[3],
                        circuit_id=record[4],
                        charger_id=record[5],
                        provider=record[6],
                        energy_area=energy_area,
                        meter_nr=meter_nr,
                    )
                )
    return data

def fetch_charger_data(charging_unit: ChargingUnit, obs_id) -> List[ObsData]:
    if obs_id["provider"] != charging_unit.provider:
        return []
    credential = AzureNamedKeyCredential(AZ_ACCOUNT, AZ_KEY)
    table_client = TableClient(
        credential=credential, endpoint=TABLE_ENDPOINT, table_name=obs_id["table"]
    )

    now = datetime(2025, 1, 31, 23, 59, 59)
    start_time = datetime(2024, 12, 1, 0, 0, 0)
    now_iso = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    start_iso = start_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    filter_str = (
        f"PartitionKey eq '{charging_unit.provider if charging_unit.provider != "GARO" else "EMABLER"}_{obs_id["obs"]}' "
        f"and RowKey gt '{charging_unit.charger_id}_{start_iso}'"
        f"and RowKey lt '{charging_unit.charger_id}_{now_iso}'"
    )

    entities = table_client.query_entities(filter_str)
    data = [
        ObsData(
            charger_id=e["chargerId"],
            timestamp=e["pulseTimestamp"],
            value=(float(e["value"])),
            observation_id=e["observationId"],
        )
        for e in entities
    ]
    return data

def fetch_nordpool_data():
    data = {}
    print("Fetching data from Nordpool")
    for day_back in range(DAYS_BACK_IN_TIME + 1):

        date = (datetime.now() - timedelta(days=day_back)).strftime("%Y-%m-%d")

        url = f"""https://dataportal-api.nordpoolgroup.com/api/DayAheadPrices?date={date}&market=DayAhead&deliveryArea=NO1,NO2,NO3,NO4,NO5&currency=NOK"""

        response = requests.request("GET", url)

        res = response.json()
        for d in res["multiAreaEntries"]:
            data[d["deliveryStart"]] = d["entryPerArea"]
    return data

def merge_data(
    charging_units: List[ChargingUnit], obs_data_list: List[ObsData], norddata, holidays
) -> List[ChargingUnitComplete]:
    complete_data = []
    unit_map = {cu.charger_id: cu for cu in charging_units}
    for obs in tqdm(obs_data_list):
        cu = unit_map.get(obs.charger_id, None)
        if cu:
            isHoliday = obs.timestamp.split("T")[0] in holidays
            entryPerArea = norddata.get(obs.timestamp)
            try:
                price = entryPerArea[cu.energy_area]
                complete_data.append(
                    ChargingUnitComplete(
                        **cu.__dict__,
                        timestamp=obs.timestamp,
                        value_kWh=float(obs.value),
                        spot_price=price,
                        is_holiday=isHoliday,
                    )
                )
            except Exception as e:
                continue

    return complete_data

def get_holidays():
    data = []
    years = [2025, 2024]
    for year in years:
        response = requests.request(
            "GET", f"https://date.nager.at/api/v3/publicholidays/{year}/NO"
        )
        res = response.json()
        data.extend(map(lambda x: x["date"], res))
    return data

def get_nordpool_price(norddata: dict, timestamp: str, energy_area: str) -> float:
    """Get price for specific timestamp and energy area from norddata."""
    try:
        entry_per_area = norddata.get(timestamp)
        if entry_per_area:
            return entry_per_area.get(energy_area)
    except Exception:
        return None
    return None

def calculate_kwh(obs_results, charging_units_df):
    obs_results_df = pd.DataFrame([d.__dict__ for d in obs_results])
    obs_results_df["timestamp"] = pd.to_datetime(
        obs_results_df["timestamp"], format="ISO8601"
    )
    obs_results_df["timestamp"] = obs_results_df["timestamp"].dt.tz_convert(
        "Europe/Oslo"
    )
    obs_results_df["floor_timestamp"] = obs_results_df["timestamp"].dt.floor("h")

    obs_results_df = obs_results_df.sort_values(["charger_id", "timestamp"])

    obs_results_df["time_diff_seconds"] = (
        obs_results_df.groupby("charger_id")["timestamp"]
        .diff()
        .shift(-1)
        .dt.total_seconds()
        .abs()
    )
    obs_results_df["prev_time_diff_seconds"] = (
        obs_results_df.groupby("charger_id")["timestamp"]
        .diff()
        .shift(1)
        .dt.total_seconds()
        .abs()
    )

    obs_results_df["next_floor_timestamp"] = obs_results_df["floor_timestamp"].shift(-1)
    obs_results_df["next_timestamp"] = obs_results_df["timestamp"].shift(-1)

    obs_results_df["prev_floor_timestamp"] = obs_results_df["floor_timestamp"].shift(1)
    obs_results_df["prev_timestamp"] = obs_results_df["timestamp"].shift(1)
    obs_results_df["prev_value"] = obs_results_df["value"].shift(1)

    condition = (obs_results_df["time_diff_seconds"] < 3600) & (
        obs_results_df["floor_timestamp"] != obs_results_df["next_floor_timestamp"]
    )

    obs_results_df.loc[condition, "time_diff_seconds"] = (
        obs_results_df["next_floor_timestamp"] - obs_results_df["timestamp"]
    ).dt.total_seconds()

    condition = (obs_results_df["prev_time_diff_seconds"] < 3600) & (
        obs_results_df["floor_timestamp"] != obs_results_df["prev_floor_timestamp"]
    )

    new_rows = obs_results_df[condition].copy()
    new_rows["time_diff_seconds"] = (
        new_rows["timestamp"] - new_rows["floor_timestamp"]
    ).dt.total_seconds()
    new_rows["timestamp"] = new_rows["floor_timestamp"]
    new_rows["value"] = new_rows["prev_value"]
    obs_results_df = pd.concat([obs_results_df, new_rows])
    obs_results_df = obs_results_df.sort_values(["charger_id", "timestamp"])


    obs_results_df["value"] = pd.to_numeric(obs_results_df["value"], errors="coerce")
    obs_results_df["value"] = obs_results_df["value"] * (
        obs_results_df["time_diff_seconds"] / 3600
    )

    result_df = (
        obs_results_df.groupby(["charger_id", "floor_timestamp"], as_index=False)[
            "value"
        ]
        .sum()
        .rename(columns={"floor_timestamp": "timestamp"})
    )

    result_df = pd.merge(result_df, charging_units_df, how="left", on="charger_id")

    return result_df


def process_data():
    charging_units = get_charging_unit_data()
    norddata = fetch_nordpool_data()
    holidays = get_holidays()

    obs_results = []
    print("Fetching data from Storage")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(fetch_charger_data, cu, obs_id)
            for cu in charging_units
            for obs_id in OBS_IDS
        ]
        for future in tqdm(
            concurrent.futures.as_completed(futures), total=len(futures)
        ):
            obs_results.extend(future.result())
    if not obs_results:
        print("No data found")
        return

    charging_units_df = pd.DataFrame([d.__dict__ for d in charging_units])

    df = calculate_kwh(obs_results, charging_units_df)
    df = df.groupby(
        [col for col in df.columns if col != "value"],
        as_index=False,
    ).agg(
        value=("value", "sum"),
    )

    df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["spot_price"] = df.apply(
        lambda row: get_nordpool_price(norddata, row["timestamp"], row["energy_area"]),
        axis=1,
    )
    df["is_holiday"] = df["timestamp"].apply(lambda x: int(x.split("T")[0] in holidays))

    total_units_per_site = (
        charging_units_df.groupby("site_id").size().reset_index(name="total_units")
    )
    df["total_units"] = df["site_id"].map(
        total_units_per_site.set_index("site_id")["total_units"]
    )

    df.to_csv("data/main/charger.csv", index=False)


if __name__ == "__main__":
    process_data()
