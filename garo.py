import calendar
import concurrent.futures
import csv
import math
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import psycopg
from azure.core.credentials import AzureNamedKeyCredential
from azure.data.tables import TableClient
from dotenv import load_dotenv

from fetchChargerData import (
    aggregate_data,
    calculate_highest_peaks_avg,
    fetch_charger_data,
    merge_data,
    plot_data,
    prepare_hourly_data,
    save_data,
)

# Load environment variables
load_dotenv()


# Constants
DB_URL = os.getenv("DB_URL")
AZ_ACCOUNT = os.getenv("AZ_ACCOUNT")
AZ_KEY = os.getenv("AZ_KEY")
TABLE_ENDPOINT = "https://pulsehubstoraged998526f.table.core.windows.net"
TABLE_NAME = "emablerlog"
PROVIDER = "GARO"
OBS_IDS = ["-36"]
DAYS_BACK_IN_TIME = 30
IDENTIFIER = "backplateName"


@dataclass
class ChargingUnit:
    site_id: str
    site_key: str
    name: str
    site_fuse_rating_a: Optional[float]
    net_type: Optional[str]
    net_v: Optional[int]
    site_power_kw: Optional[float]
    has_loadbalancer: Optional[bool]
    circuit_id: Optional[str]
    circuit_name: Optional[str]
    circuit_fuse_rating_a: Optional[float]
    circuit_power_kw: Optional[float]
    provider: str
    charger_id: str


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
    site_fuse_rating_a: Optional[float]
    net_type: Optional[str]
    net_v: Optional[int]
    site_power_kw: Optional[float]
    has_loadbalancer: Optional[bool]
    circuit_id: Optional[str]
    circuit_name: Optional[str]
    circuit_fuse_rating_a: Optional[float]
    circuit_power_kw: Optional[float]
    provider: str
    charger_id: str
    timestamp: str
    value_kWh: str
    observation_id: str


def get_charging_unit_data() -> List[ChargingUnit]:
    query = f"""
        SELECT
            "PulseStructureSite".id AS "siteId",
            "PulseStructureSite"."siteKey",
            "PulseStructureSite"."name",
            "PulseStructureSite"."fuseRating" AS "siteFuseRating(A)",
            "PulseStructureSite"."netType",
            REGEXP_REPLACE("PulseStructureSite"."netType", '[^0-9]', '', 'g')::INTEGER AS "net(V)",
            REGEXP_REPLACE("PulseStructureSite"."netType", '[^0-9]', '', 'g')::INTEGER * "PulseStructureSite"."fuseRating" * SQRT(3) / 1000 AS "sitePower(kW)",
            "PulseStructureSite"."hasLoadbalancer",
            "PulseStructureCircuit".id AS "circuitId",
            "PulseStructureCircuit"."name" AS "circuitName",
            "PulseStructureCircuit"."fuseRating" AS "circuitFuseRating(A)",
            "PulseStructureChargingUnit".provider,
            "PulseStructureChargingUnit"."{IDENTIFIER}",
            REGEXP_REPLACE("PulseStructureSite"."netType", '[^0-9]', '', 'g')::INTEGER * "PulseStructureCircuit"."fuseRating" * SQRT(3) / 1000 AS "circuitPower(kW)"
        FROM
            "PulseStructureSite"
            LEFT JOIN "PulseStructureCircuit" ON "PulseStructureSite".id = "PulseStructureCircuit"."siteId"
            LEFT JOIN "PulseStructureChargingUnit" ON "PulseStructureCircuit".id = "PulseStructureChargingUnit"."circuitId"
        WHERE
            "PulseStructureChargingUnit".provider = '{PROVIDER}'
            AND "PulseStructureSite"."siteKey" IN ('ca4acfcb-381a-4f23-b677-9314804be5bf')
    """
    data = []
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            for record in cur:
                data.append(
                    ChargingUnit(
                        site_id=record[0],
                        site_key=record[1],
                        name=record[2],
                        site_fuse_rating_a=record[3],
                        net_type=record[4],
                        net_v=record[5],
                        site_power_kw=record[6],
                        has_loadbalancer=record[7],
                        circuit_id=record[8],
                        circuit_name=record[9],
                        circuit_fuse_rating_a=record[10],
                        provider=record[11],
                        charger_id=record[12],
                        circuit_power_kw=record[13],
                    )
                )
    return data


def process_data():
    # Fetch static charging unit info
    charging_units = get_charging_unit_data()
    # Fetch observation data (parallel)
    obs_results = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                fetch_charger_data, cu, obs_id, DAYS_BACK_IN_TIME, "EMABLER", TABLE_NAME
            )
            for cu in charging_units
            for obs_id in OBS_IDS
        ]
        for future in concurrent.futures.as_completed(futures):
            obs_results.extend(future.result())

    if not obs_results:
        print("No data found")
        return

    # Merge data
    complete_data = merge_data(charging_units, obs_results)
    if not complete_data:
        print("No valid observation data found.")
        return

    df = pd.DataFrame([d.__dict__ for d in complete_data])

    save_data(df, charging_units, PROVIDER)


if __name__ == "__main__":
    process_data()
