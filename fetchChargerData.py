import calendar
from datetime import datetime, timedelta
import math
import os
import numpy as np
import pandas as pd
from azure.data.tables import TableClient
from azure.core.credentials import AzureNamedKeyCredential
import csv
import concurrent.futures
import psycopg
from dataclasses import dataclass
from typing import Optional, List
import plotly.graph_objects as go
from dotenv import load_dotenv

# Load environment variables
load_dotenv()



# Constants
DB_URL = os.getenv("DB_URL")
AZ_ACCOUNT = os.getenv("AZ_ACCOUNT")
AZ_KEY = os.getenv("AZ_KEY")
TABLE_ENDPOINT = "https://pulsehubstoraged998526f.table.core.windows.net"
TABLE_NAME = "formattedobservations"
PROVIDER = "EASEE"
SITE_NAME_FILTER = "Prestestien Brl%"
OBS_IDS = ["122"]
DAYS_BACK_IN_TIME = 30


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
    """
    Fetch charging unit data from the database.
    Only fetches units from the specified provider and site key.
    """
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
            "PulseStructureChargingUnit"."chargerId",
            REGEXP_REPLACE("PulseStructureSite"."netType", '[^0-9]', '', 'g')::INTEGER * "PulseStructureCircuit"."fuseRating" * SQRT(3) / 1000 AS "circuitPower(kW)"
        FROM
            "PulseStructureSite"
            LEFT JOIN "PulseStructureCircuit" ON "PulseStructureSite".id = "PulseStructureCircuit"."siteId"
            LEFT JOIN "PulseStructureChargingUnit" ON "PulseStructureCircuit".id = "PulseStructureChargingUnit"."circuitId"
        WHERE
            "PulseStructureChargingUnit".provider = '{PROVIDER}'
            AND "PulseStructureSite"."siteKey" in ('RYW8-C322', '8N4M-N722', 'ZQK6-W522', '6M6M-7222', '28T4-7722', 'TRED-E222', 'EMRB-G222')


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


def fetch_charger_data(
    charging_unit: ChargingUnit, obs_id: str, days_back: int
) -> List[ObsData]:
    """
    Fetch charger observation data from Azure Table Storage for a single charger and observation ID
    within a specified timeframe.
    """
    if charging_unit.provider != PROVIDER:
        return []

    credential = AzureNamedKeyCredential(AZ_ACCOUNT, AZ_KEY)
    table_client = TableClient(
        credential=credential, endpoint=TABLE_ENDPOINT, table_name=TABLE_NAME
    )

    now = datetime.now()
    start_time = now - timedelta(days=days_back)
    now_iso = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    start_iso = start_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    filter_str = (
        f"PartitionKey eq 'EASEE_{obs_id}' "
        f"and RowKey gt '{charging_unit.charger_id}_{start_iso}' "
        f"and RowKey lt '{charging_unit.charger_id}_{now_iso}'"
    )

    entities = table_client.query_entities(filter_str)
    return [
        ObsData(
            charger_id=e["chargerId"],
            timestamp=e["pulseTimestamp"],
            value=e["value"],
            observation_id=e["observationId"],
        )
        for e in entities
    ]


def merge_data(
    charging_units: List[ChargingUnit], obs_data_list: List[ObsData]
) -> List[ChargingUnitComplete]:
    """
    Merge charging unit static info with observation data.
    Only include entries where value > 0.
    """
    complete_data = []
    unit_map = {cu.charger_id: cu for cu in charging_units}
    for obs in obs_data_list:
        if float(obs.value) > 0:
            cu = unit_map.get(obs.charger_id)
            if cu:
                complete_data.append(
                    ChargingUnitComplete(
                        **cu.__dict__,
                        timestamp=obs.timestamp,
                        value_kWh=obs.value,
                        observation_id=obs.observation_id,
                    )
                )
    return complete_data


def aggregate_data(df: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
    """
    Aggregate the raw merged data at multiple levels:
    - Site-Circuit-Timestamp: sum of kWh and count of chargers.
    - Site-Timestamp: sum of all circuits at the site for each timestamp.

    Returns:
        report_circuit_level: Aggregated at site_key, name, circuit_id, timestamp.
        report_site_level: Aggregated at site_key, timestamp to get total usage per hour at the site.
    """
    # Ensure correct types
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["value_kWh"] = pd.to_numeric(df["value_kWh"])
    df["circuit_power_kw"] = pd.to_numeric(df["circuit_power_kw"])

    # Circuit-level aggregation by hour
    report_circuit_level = (
        df.groupby(["site_key", "name", "circuit_id", "timestamp"])
        .agg(sum_value_kWh=("value_kWh", "sum"), chargers=("value_kWh", "count"))
        .reset_index()
    )

    # Site-level aggregation by hour (sum over all circuits)
    report_site_level = report_circuit_level.groupby(
        ["site_key", "timestamp"], as_index=False
    ).agg(site_sum_value_kWh=("sum_value_kWh", "sum"))

    return report_circuit_level, report_site_level


def calculate_highest_peaks_avg(
    df: pd.DataFrame, group_cols: List[str], value_col: str
) -> pd.DataFrame:
    """
    Calculate the average of the top 3 highest peaks for each group in a DataFrame.

    Arguments:
        df: Input DataFrame.
        group_cols: Columns to group by.
        value_col: The column with values to consider for top peaks.

    Returns:
        A DataFrame with group_cols plus a new column 'avg_three_highest_peaks'.
    """

    def top_three_avg(group):
        top_three = group.nlargest(3, value_col)[value_col]
        return (
            top_three.mean()
            if len(top_three) == 3
            else top_three.mean() if len(top_three) > 0 else 0
        )

    result = (
        df.groupby(group_cols)
        .apply(top_three_avg)
        .reset_index(name="avg_three_highest_peaks")
        .sort_values("avg_three_highest_peaks", ascending=False)
    )
    return result


def prepare_hourly_data(
    report_df: pd.DataFrame, df_original: pd.DataFrame
) -> pd.DataFrame:
    """
    For each site and circuit, create a full grid of (day, hour) combinations for the given period
    and merge with the existing report data to fill missing hours with zeros.
    """
    # Extract day and hour from timestamp
    report_df["day"] = report_df["timestamp"].dt.day
    report_df["hour"] = report_df["timestamp"].dt.hour.astype(int)

    # Group by day/hour and take the mean (or sum) as needed
    # For circuit level we keep sum_value_kWh and chargers aggregated
    hourly_data = report_df.groupby(
        ["site_key", "name", "circuit_id", "day", "hour"], as_index=False
    ).agg(sum_value_kWh=("sum_value_kWh", "mean"), chargers=("chargers", "mean"))

    # Add circuit power info back
    circuit_power_info = df_original[
        ["site_key", "circuit_id", "circuit_power_kw"]
    ].drop_duplicates()
    hourly_data = hourly_data.merge(
        circuit_power_info, on=["site_key", "circuit_id"], how="left"
    )

    # Calculate ratio
    hourly_data["used_power_ratio"] = (
        (hourly_data["sum_value_kWh"] / hourly_data["circuit_power_kw"] * 100)
        .fillna(0)
        .round(2)
    )

    # Expand data to include all possible day-hour combinations for each site-circuit
    expanded_data = []
    for (site, circuit), sub_df in hourly_data.groupby(["site_key", "circuit_id"]):
        # Assume 31 days and 24 hours as in original code
        all_combinations = pd.MultiIndex.from_product(
            [[site], [circuit], range(1, 32), range(24)],
            names=["site_key", "circuit_id", "day", "hour"],
        ).to_frame(index=False)

        merged = pd.merge(
            all_combinations,
            sub_df,
            on=["site_key", "circuit_id", "day", "hour"],
            how="left",
        ).fillna(
            {
                "sum_value_kWh": 0,
                "chargers": 0,
                "circuit_power_kw": 0,
                "used_power_ratio": 0,
            }
        )

        expanded_data.append(merged)

    expanded_data = pd.concat(expanded_data, ignore_index=True).sort_values(
        by=["site_key", "circuit_id", "day", "hour"]
    )

    return expanded_data


def plot_data(
    report_expanded: pd.DataFrame,
    site_names: pd.DataFrame,
    circuit_highest_peaks: pd.DataFrame,
):
    """
    Plot the expanded hourly data.
    Each circuit trace will show the circuit-level avg_three_highest_peaks in the legend.
    """
    fig = go.Figure()

    # Iterate over sites
    for site in report_expanded["site_key"].unique():
        site_data = report_expanded[report_expanded["site_key"] == site]
        site_name = site_names[site_names["site_key"] == site]["name"].values[0]

        # Create a dummy empty trace for the site group for improved legend grouping
        fig.add_trace(
            go.Bar(
                x=[],
                y=[],
                name=f"Site {site}",
                legendgroup=f"site_{site}",
                showlegend=True,
            )
        )

        # Iterate over circuits
        for circuit in site_data["circuit_id"].unique():
            circuit_data = site_data[site_data["circuit_id"] == circuit]

            # Get the avg_three_highest_peaks for this specific circuit
            avg_peak_for_circuit = circuit_highest_peaks[
                (circuit_highest_peaks["site_key"] == site)
                & (circuit_highest_peaks["circuit_id"] == circuit)
            ]["avg_three_highest_peaks"].values

            if len(avg_peak_for_circuit) > 0:
                avg_peak_str = f"{avg_peak_for_circuit[0]:.2f} kWh"
            else:
                avg_peak_str = "N/A"

            fig.add_trace(
                go.Bar(
                    x=[
                        [f"{day}" for day in circuit_data["day"]],
                        [f"{hour:02}" for hour in circuit_data["hour"]],
                    ],
                    y=circuit_data["sum_value_kWh"],
                    name=f"Circuit {circuit}",
                    legendgroup=f"site_{site}",
                    legendgrouptitle_text=f"Site {site_name}",
                    showlegend=True,
                    text=circuit_data["chargers"],
                    textposition="outside",
                    textfont=dict(size=14),
                    hovertext=f"Avg top 3 peaks: {avg_peak_str}",
                )
            )

    fig.update_layout(
        legend=dict(groupclick="toggleitem"),
        barmode="group",
        yaxis_title="ΣEnergy (kWh)",
    )

    fig.show()
    fig.write_html("index.html")


def process_data():
    # Fetch static charging unit info
    charging_units = get_charging_unit_data()

    # Fetch observation data (parallel)
    obs_results = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(fetch_charger_data, cu, obs_id, DAYS_BACK_IN_TIME)
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

    old_df = df.copy()

    # Aggregate data at circuit and site levels
    report_circuit_level, report_site_level = aggregate_data(df)

    # Calculate top 3 peaks average at circuit level
    # We use site_key and circuit_id as grouping columns
    circuit_highest_peaks = calculate_highest_peaks_avg(
        report_circuit_level,
        group_cols=["site_key", "circuit_id"],
        value_col="sum_value_kWh",
    )

    # Calculate top 3 peaks average at site level (considering total usage)
    site_highest_peaks = calculate_highest_peaks_avg(
        report_site_level, group_cols=["site_key"], value_col="site_sum_value_kWh"
    )

    # Prepare hourly data (expanding to all day-hour combinations)
    # We'll work with circuit-level report here
    expanded_data = prepare_hourly_data(report_circuit_level, df)

    # Extract unique site names for the plotting function
    site_names = df[["site_key", "name"]].drop_duplicates()

    # Plot the data
    # Now each circuit trace hovertext will contain its avg_three_highest_peaks value
    plot_data(expanded_data, site_names, circuit_highest_peaks)

    # Write highest peaks averages to CSV for reference
    site_highest_peaks.to_csv("data/site_highest_peaks_avg.csv", index=False)
    circuit_highest_peaks.to_csv("data/circuit_highest_peaks_avg.csv", index=False)

    old_df["timestamp"] = pd.to_datetime(old_df["timestamp"])
    old_df.set_index("timestamp", inplace=True)

    # Resample from noon to noon:
    # '24H' sets the length of each interval to 24 hours,
    # 'offset="12H"' means the period starts at 12:00 (noon) each day.
    daily_sums_circuits = (
        old_df.groupby("circuit_id")
        .resample("24h", offset="12h")["value_kWh"]
        .sum()
        .reset_index()
    )
    daily_sums_circuits["day_of_week"] = daily_sums_circuits["timestamp"].dt.day_name()
    daily_sums_circuits = daily_sums_circuits[
        ["circuit_id", "timestamp", "day_of_week", "value_kWh"]
    ]

    report_circuit_level_avg = report_circuit_level.groupby(
        ["site_key", "name", "circuit_id"], as_index=False
    ).agg(avg_value_kWh=("sum_value_kWh", "mean"))

    report_circuit_level_avg['avg_value_kWh'] = np.ceil(report_circuit_level_avg['avg_value_kWh']) + 1
    report_circuit_level_avg.to_csv("data/report_circuit_level_avg.csv")
    daily_sums_circuits.to_csv("data/daily_sums_circuits.csv")

    daily_sums = old_df.resample("24h", offset="12h")["value_kWh"].sum()
    daily_sums.to_csv("data/daily_sums.csv")


if __name__ == "__main__":
    process_data()
