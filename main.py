from datetime import datetime, timedelta
import os
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
TABLE_NAME = "formattedobservations"
OBS_IDS = [{"provider": "EASEE", "obs": "120"}, {"provider": "ZAPTEC", "obs": "513"}]
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


def get_charging_unit_data() -> List[ChargingUnit]:
    #            AND ("PulseStructureSite"."siteKey" = '925b00ad-71d9-44f3-a821-faf18303eb73' OR "PulseStructureSite"."siteKey" = 'RYW8-C322')

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
            ("PulseStructureChargingUnit".provider = 'EASEE' 
            OR "PulseStructureChargingUnit".provider = 'ZAPTEC')
            AND "PulseStructureChargingUnit"."SF_active" = TRUE
            AND "PulseStructureSite"."salesforceId" IS NOT NULL
        ),
        counter AS (
            SELECT "siteKey", COUNT("chargerId") as "count" FROM raw GROUP BY "siteKey"
        )
        SELECT * FROM raw WHERE "siteKey" IN (SELECT "siteKey" FROM counter WHERE "count" <= 10) AND "sitePower(kW)" IS NOT NULL
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
                    )
                )
    return data


def fetch_charger_data(charging_unit: ChargingUnit, obs_id) -> List[ObsData]:
    if obs_id["provider"] != charging_unit.provider:
        return []
    credential = AzureNamedKeyCredential(AZ_ACCOUNT, AZ_KEY)
    table_client = TableClient(
        credential=credential, endpoint=TABLE_ENDPOINT, table_name=TABLE_NAME
    )

    now = datetime.now()
    start_time = now - timedelta(days=DAYS_BACK_IN_TIME)
    now_iso = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    start_iso = start_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    filter_str = (
        f"PartitionKey eq '{charging_unit.provider}_{obs_id["obs"]}' "
        f"and RowKey gt '{charging_unit.charger_id}_{start_iso}' "
        f"and RowKey lt '{charging_unit.charger_id}_{now_iso}'"
    )

    entities = table_client.query_entities(filter_str)
    data = [
        ObsData(
            charger_id=e["chargerId"],
            timestamp=e["pulseTimestamp"],
            value=(
                float(e["value"]) / 1000.0
                if obs_id["provider"] == "ZAPTEC"
                else float(e["value"])
            ),
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
                        value_kWh=obs.value,
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



def fill_timestamp(df):
    # First, create a reference DataFrame with static values for each charger_id
    static_columns = [
        "site_id",
        "site_key",
        "name",
        "observation_id",
        "site_power",
        "circuit_id",
        "provider",
        "energy_area",
    ]
    static_values = (
        df[["charger_id"] + static_columns]
        .drop_duplicates("charger_id")
        .set_index("charger_id")
    )

    # # Create complete date range
    min_date = df["timestamp"].min()
    max_date = df["timestamp"].max()
    all_timestamps = pd.date_range(start=min_date, end=max_date, freq="h")

    # Create all possible combinations
    charger_ids = df["charger_id"].unique()
    index = pd.MultiIndex.from_product(
        [charger_ids, all_timestamps], names=["charger_id", "timestamp"]
    )

    # Create new DataFrame with all combinations
    df_expanded = pd.DataFrame(index=index).reset_index()

    # Merge static values
    df_expanded = df_expanded.merge(
        static_values, left_on="charger_id", right_index=True
    )

    # Merge with original values
    df = df.set_index(["charger_id", "timestamp"])
    df_expanded = df_expanded.set_index(["charger_id", "timestamp"])
    df_expanded["value"] = df["value"]
    df_expanded["value"] = df_expanded["value"].fillna(0)

    # Reset index and continue with your existing code
    df = df_expanded.reset_index()
    return df

def calculate_kwh(obs_results, charging_units_df):
    obs_results_df = pd.DataFrame([d.__dict__ for d in obs_results])
    obs_results_df["timestamp"] = pd.to_datetime(
        obs_results_df["timestamp"], format="ISO8601"
    )
    obs_results_df["floor_timesamp"] = obs_results_df["timestamp"].dt.floor("h")
    obs_results_df["time_diff_seconds"] = (
        None  # Start with an empty column for time differences
    )

    # Iterate over each group and calculate the time difference within the same group
    for _, group in obs_results_df.groupby(["charger_id", "floor_timesamp"]):
        group["time_diff_seconds"] = (
            group["timestamp"].shift(-1) - group["timestamp"]
        ).dt.total_seconds()
        group.loc[group.index[-1], "time_diff_seconds"] = (
            0  # Set the last value to 0
        )

        obs_results_df.loc[group.index, "time_diff_seconds"] = group[
            "time_diff_seconds"
        ]

    obs_results_df["value"] = pd.to_numeric(
        obs_results_df["value"], errors="coerce"
    )
    obs_results_df["time_diff_seconds"] = pd.to_numeric(
        obs_results_df["time_diff_seconds"], errors="coerce"
    )

    obs_results_df["value"] = obs_results_df["value"] * (
        obs_results_df["time_diff_seconds"] / 3600
    )

    obs_results_df["timestamp"] = obs_results_df["floor_timesamp"]
    obs_results_df = obs_results_df.drop(
        columns=["time_diff_seconds", "floor_timesamp"]
    )

    df = pd.merge(obs_results_df, charging_units_df, how="left", on="charger_id")
    df["value"] = df["value"].astype(float)
    return df


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
        # Wrap the `as_completed` iterator in tqdm, passing the total number of futures
        for future in tqdm(
            concurrent.futures.as_completed(futures), total=len(futures)
        ):
            obs_results.extend(future.result())
    if not obs_results:
        print("No data found")
        return

    charging_units_df = pd.DataFrame([d.__dict__ for d in charging_units])

    total_units_per_site = (
        charging_units_df.groupby("site_id").size().reset_index(name="total_units")
    )

    df = calculate_kwh(obs_results, charging_units_df)
    df = df.groupby(
        [col for col in df.columns if col != "value"],
        as_index=False,
    ).agg(
        value=("value", "sum"),
    )

    df = fill_timestamp(df)

    df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["spot_price"] = df.apply(
        lambda row: get_nordpool_price(norddata, row["timestamp"], row["energy_area"]),
        axis=1,
    )
    df["is_holiday"] = df["timestamp"].apply(lambda x: int(x.split("T")[0] in holidays))
    df["total_units"] = df["site_id"].map(
        total_units_per_site.set_index("site_id")["total_units"]
    )

    df.to_csv("forcast/charger.csv", index=False)
    df_site = (
        df.groupby(
            [
                "site_id",
                "site_key",
                "name",
                "provider",
                "energy_area",
                "spot_price",
                "site_power",
                "is_holiday",
                "timestamp",
                "total_units",
            ],
            as_index=False,
        )
        .agg(
            value=("value", "sum"),
            charging_units=("value", lambda x: (x != 0).sum()),
        )
        .sort_values(by=["site_key", "timestamp"])
    )

    df_site.to_csv("forcast/site.csv", index=False)

    df_circuit= (
        df.groupby(
            [
                "site_id",
                "site_key",
                "name",
                "provider",
                "energy_area",
                "spot_price",
                "site_power",
                "is_holiday",
                "circuit_id",
                "timestamp",
                "total_units",
            ],
            as_index=False,
        )
        .agg(
            value=("value", "sum"),
            charging_units=("value", lambda x: (x != 0).sum()),
        )
        .sort_values(by=["site_key", "circuit_id", "timestamp"])
    )
    df_circuit.to_csv("forcast/circuit.csv", index=False)



if __name__ == "__main__":
    process_data()
