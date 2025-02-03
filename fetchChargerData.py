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
from tqdm import tqdm

# Load environment variables
load_dotenv()


# Constants
DB_URL = os.getenv("DB_URL")
AZ_ACCOUNT = os.getenv("AZ_ACCOUNT")
AZ_KEY = os.getenv("AZ_KEY")
TABLE_ENDPOINT = "https://pulsehubstoraged998526f.table.core.windows.net"
TABLE_NAME = "formattedobservations"
PROVIDER = "EASEE"
OBS_IDS = ["122"]
DAYS_BACK_IN_TIME = 60


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
    value_kW: str
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
            AND "PulseStructureSite"."siteKey" in ('S988-X622')



    """
    data = []
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            for record in tqdm(cur):
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
    charging_unit: ChargingUnit,
    obs_id: str,
    days_back: int,
    provider: str,
    table_name: str,
) -> List[ObsData]:
    """
    Fetch charger observation data from Azure Table Storage for a single charger and observation ID
    within a specified timeframe.
    """
    if charging_unit.provider != provider and provider != "EMABLER":
        return []

    credential = AzureNamedKeyCredential(AZ_ACCOUNT, AZ_KEY)
    table_client = TableClient(
        credential=credential, endpoint=TABLE_ENDPOINT, table_name=table_name
    )

    now = datetime(2025, 1, 31, 23, 59, 59)
    start_time = datetime(2025, 1, 1, 0, 0, 0)
    now_iso = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    start_iso = start_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    filter_str = (
        f"PartitionKey eq '{provider}_{obs_id}' "
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
    for obs in tqdm(obs_data_list):
        if float(obs.value) > 0:
            cu = unit_map.get(obs.charger_id)
            if cu:
                complete_data.append(
                    ChargingUnitComplete(
                        **cu.__dict__,
                        timestamp=obs.timestamp,
                        value_kW=(
                            float(obs.value) / 1000
                            if float(obs.value) > 30
                            else float(obs.value)
                        ),
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
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601")
    df["value_kW"] = pd.to_numeric(df["value_kW"])
    df["circuit_power_kw"] = pd.to_numeric(df["circuit_power_kw"])

    # Circuit-level aggregation by hour
    report_circuit_level = (
        df.groupby(["site_key", "name", "circuit_id", "timestamp"])
        .agg(sum_value=("value_kW", "sum"), chargers=("value_kW", "count"))
        .reset_index()
    )

    report_site_level = (
        df.groupby(["site_key", "name", "timestamp"])
        .agg(sum_value=("value_kW", "sum"), chargers=("value_kW", "count"))
        .reset_index()
    )

    return report_circuit_level, report_site_level


def calculate_highest_peaks_avg(
    df: pd.DataFrame, group_cols: List[str], value_col: str
) -> pd.DataFrame:
    """
    Calculate the highest peak for each day, then the avg of the three highest peaks for each group in a DataFrame.

    Arguments:
        df: Input DataFrame.
        group_cols: Columns to group by.
        value_col: The column with values to consider for top peaks.

    Returns:
        A DataFrame with group_cols plus a new column 'avg_three_highest_peaks'.
    """

    # Calculate the highest peak for each day
    highest_peaks = (
        df.groupby(group_cols + [df["timestamp"].dt.date])
        .agg(max_value=(value_col, "max"))
        .reset_index()
    )

    # Calculate the average of the three highest peaks for each group
    highest_peaks["rank"] = highest_peaks.groupby(group_cols)["max_value"].rank(
        method="max", ascending=False
    )
    highest_peaks = highest_peaks[highest_peaks["rank"] <= 3]

    avg_three_highest_peaks = (
        highest_peaks.groupby(group_cols)["max_value"]
        .mean()
        .reset_index(name="avg_three_highest_peaks")
    )

    return avg_three_highest_peaks


def prepare_hourly_data(
    report_df: pd.DataFrame, df_original: pd.DataFrame
) -> pd.DataFrame:
    """
    For each site and circuit, create a full grid of (day, hour) combinations for the given period
    and merge with the existing report data to fill missing hours with zeros.
    """
    # Group by day/hour and take the mean (or sum) as needed
    # For circuit level we keep sum_value and chargers aggregated
    report_df["timestamp"] = report_df["timestamp"].dt.floor("h")
    hourly_data = report_df.groupby(
        ["site_key", "name", "circuit_id", "timestamp"], as_index=False
    ).agg(sum_value=("sum_value", "mean"), chargers=("chargers", "mean"))

    # Add circuit power info back
    circuit_power_info = df_original[
        ["site_key", "circuit_id", "circuit_power_kw"]
    ].drop_duplicates()
    hourly_data = hourly_data.merge(
        circuit_power_info, on=["site_key", "circuit_id"], how="left"
    )

    # Calculate ratio
    hourly_data["used_power_ratio"] = (
        (hourly_data["sum_value"] / hourly_data["circuit_power_kw"] * 100)
        .fillna(0)
        .round(2)
    )

    # Expand data to include all possible day-hour combinations for each site-circuit
    expanded_data = []

    range_dates = pd.date_range(
        start=hourly_data["timestamp"].min().normalize(),
        end=hourly_data["timestamp"].max().normalize(),
        freq="h",
    )

    for (site, circuit), sub_df in hourly_data.groupby(["site_key", "circuit_id"]):
        # Assume 31 days and 24 hours as in original code
        all_combinations = pd.MultiIndex.from_product(
            [[site], [circuit], range_dates],
            names=["site_key", "circuit_id", "timestamp"],
        ).to_frame(index=False)

        merged = pd.merge(
            all_combinations,
            sub_df,
            on=["site_key", "circuit_id", "timestamp"],
            how="left",
        ).fillna(
            {
                "sum_value": 0,
                "chargers": 0,
                "circuit_power_kw": 0,
                "used_power_ratio": 0,
            }
        )

        expanded_data.append(merged)

    expanded_data = pd.concat(expanded_data, ignore_index=True).sort_values(
        by=["site_key", "circuit_id", "timestamp"]
    )

    # lookup name from site_key
    expanded_data["name"] = expanded_data["site_key"].apply(
        lambda x: df_original[df_original["site_key"] == x]["name"].values[0]
    )

    expanded_data.to_csv("data/expanded_data.csv")

    return expanded_data


def plot_data(
    report_circuit_level: pd.DataFrame,
    df: pd.DataFrame,
):
    """
    Plot the expanded hourly data.
    Each circuit trace will show the circuit-level avg_three_highest_peaks in the legend.
    """
    circuit_highest_peaks = calculate_highest_peaks_avg(
        report_circuit_level,
        group_cols=["site_key", "circuit_id"],
        value_col="sum_value",
    )
    report_expanded = prepare_hourly_data(report_circuit_level, df)

    # Extract unique site names for the plotting function
    site_names = df[["site_key", "name"]].drop_duplicates()
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
                    x=circuit_data["timestamp"],
                    y=circuit_data["sum_value"],
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
            fig.update_xaxes(
                dtick=3600000, tickformat="%H\n%d\n%b\n%Y", tickmode="auto"
            )

    fig.update_layout(
        legend=dict(groupclick="toggleitem"),
        barmode="group",
        yaxis_title="ΣEnergy (kWh)",
    )

    fig.show()
    fig.write_html("index.html")


def plot_site_data(
    report_circuit_level: pd.DataFrame,
    df: pd.DataFrame,
):
    """
    Plot the expanded hourly data.
    Each circuit trace will show the circuit-level avg_three_highest_peaks in the legend.
    """
    report_expanded = prepare_hourly_data(report_circuit_level, df)

    # Extract unique site names for the plotting function
    site_names = df[["site_key", "name"]].drop_duplicates()
    fig = go.Figure()

    # Iterate over sites
    # Iterate over sites

    capacity_steps = {
        "Kapasitetstrinn 1": 0,
        "Kapasitetstrinn 2": 2,
        "Kapasitetstrinn 3": 5,
        "Kapasitetstrinn 4": 10,
        "Kapasitetstrinn 5": 15,
        "Kapasitetstrinn 6": 20,
        "Kapasitetstrinn 7": 25,
        "Kapasitetstrinn 8": 50,
        "Kapasitetstrinn 9": 75,
        "Kapasitetstrinn 10": 100,
    }

    for site in report_expanded["site_key"].unique():
        site_data = report_expanded[report_expanded["site_key"] == site]
        site_name = site_names[site_names["site_key"] == site]["name"].values[0]

        # Aggregate data at the site level
        site_aggregated = (
            site_data.groupby("timestamp")["sum_value"].sum().reset_index()
        )

        # Get the avg_three_highest_peaks for the entire site

        # Add site-level trace
        fig.add_trace(
            go.Bar(
                x=site_aggregated["timestamp"],
                y=site_aggregated["sum_value"],
                name=f"Site {site}",
                legendgroup=f"site_{site}",
                legendgrouptitle_text=f"Site {site_name}",
                showlegend=True,
                text=site_aggregated["sum_value"],
                textposition="outside",
                textfont=dict(size=14),
            )
        )
    for name, y_value in capacity_steps.items():
        fig.add_shape(
            type="line",
            x0=min(report_expanded["timestamp"]),  # Start of the x-axis
            x1=max(report_expanded["timestamp"]),  # End of the x-axis
            y0=y_value,
            y1=y_value,  # Horizontal line at y = kWh/h
            line=dict(color="red", width=2, dash="dash"),  # Dashed red line
        )

        # Add annotation for each step
        fig.add_annotation(
            x=max(report_expanded["timestamp"]),  # Position label at the right side
            y=y_value,
            text=name,
            showarrow=False,
            font=dict(size=12, color="black"),
            xanchor="left",
            yanchor="bottom",
        )

    fig.update_xaxes(dtick=3600000, tickformat="%H\n%d\n%b\n%Y", tickmode="auto")
    fig.show()
    fig.write_html("index.html")


def plot_site_df_data(
    report_circuit_level: pd.DataFrame,
    df: pd.DataFrame,
    site_name: str,
    start_date: datetime = None,
):
    """
    Plot the expanded hourly data.
    Each circuit trace will show the circuit-level avg_three_highest_peaks in the legend.
    """
    report_expanded = prepare_hourly_data(report_circuit_level, df)

    # Extract unique site names for the plotting function
    site_names = df[["site_key", "name"]].drop_duplicates()
    fig = go.Figure()

    # Iterate over sites
    # Iterate over sites

    capacity_steps = {
        "Kapasitetstrinn 1": 0,
        "Kapasitetstrinn 2": 2,
        "Kapasitetstrinn 3": 5,
        "Kapasitetstrinn 4": 10,
        "Kapasitetstrinn 5": 15,
        "Kapasitetstrinn 6": 20,
        "Kapasitetstrinn 7": 25,
        "Kapasitetstrinn 8": 50,
        "Kapasitetstrinn 9": 75,
        "Kapasitetstrinn 10": 100,
    }
    highest_peak = calculate_highest_peaks_avg(
        report_circuit_level,
        group_cols=["site_key"],
        value_col="sum_value",
    )["avg_three_highest_peaks"].values[0]

    # Aggregate data at the site level
    site_aggregated = (
        report_expanded.groupby("timestamp")["sum_value"].sum().reset_index()
    )

    capacity_steps_array = list(capacity_steps.values())

    if start_date is not None:
        fig.add_shape(
            type="line",
            x0=start_date,
            x1=start_date,
            y0=0,
            y1=max(site_aggregated["sum_value"]) + 10,
            line=dict(
                color="green",
                width=2,
            ),
        )

    # Get the avg_three_highest_peaks for the entire site

    # Add site-level trace

    fig.add_trace(
        go.Bar(
            x=site_aggregated["timestamp"],
            y=site_aggregated["sum_value"],
            name=f"Site {site_name}",
            legendgroup=f"site_{site_name}",
            legendgrouptitle_text=f"Site {site_name}",
            showlegend=True,
            text=site_aggregated["sum_value"],
            textposition="outside",
            textfont=dict(size=14),
        )
    )
    for i, (name, y_value) in enumerate(capacity_steps.items()):

        if highest_peak < y_value and capacity_steps_array[i - 1] <= highest_peak:
            fig.add_shape(
                type="line",
                x0=min(report_expanded["timestamp"]),  # Start of the x-axis
                x1=max(report_expanded["timestamp"]),  # End of the x-axis
                y0=y_value,
                y1=y_value,  # Horizontal line at y = kWh/h
                line=dict(color="red", width=2, dash="dash"),  # Dashed red line
            )

            # Add annotation for each step
            fig.add_annotation(
                x=max(report_expanded["timestamp"]),  # Position label at the right side
                y=y_value,
                text=name,
                showarrow=False,
                font=dict(size=12, color="black"),
                xanchor="left",
                yanchor="bottom",
            )
        if y_value <= highest_peak:
            fig.add_shape(
                type="line",
                x0=min(report_expanded["timestamp"]),  # Start of the x-axis
                x1=max(report_expanded["timestamp"]),  # End of the x-axis
                y0=y_value,
                y1=y_value,  # Horizontal line at y = kWh/h
                line=dict(color="red", width=2, dash="dash"),  # Dashed red line
            )

            # Add annotation for each step
            fig.add_annotation(
                x=max(report_expanded["timestamp"]),  # Position label at the right side
                y=y_value,
                text=name,
                showarrow=False,
                font=dict(size=12, color="black"),
                xanchor="left",
                yanchor="bottom",
            )

    fig.update_xaxes(dtick=3600000, tickformat="%H\n%d\n%b\n%Y", tickmode="auto")
    fig.show()
    fig.write_html("index.html")


def save_data(df, charging_units, provider):

    # Aggregate data at circuit and site levels
    df = df.sort_values(["charger_id", "timestamp"])
    df.to_csv(
        f"data/{provider}/chargers.csv",
        index=False,
        columns=["charger_id", "timestamp", "value_kW"],
    )
    report_circuit_level, report_site_level = aggregate_data(df)
    print("Data Aggregated")

    site_highest_peaks = calculate_highest_peaks_avg(
        report_site_level, group_cols=["site_key", "name"], value_col="sum_value"
    )
    print("Highest peaks calculated")

    site_highest_peaks.to_csv(
        f"data/{provider}/site_highest_peaks_avg.csv", index=False
    )

    # plot_data(report_circuit_level, df)
    # plot_site_data(report_circuit_level, df)
    name = report_circuit_level["name"].unique()[0]
    plot_site_df_data(
        report_circuit_level, df, name, pd.Timestamp("2025-01-14 00:00:00")
    )

    site_avg = report_site_level.groupby(["site_key", "name"], as_index=False).agg(
        avg_value_kW=("sum_value", "mean")
    )

    site_avg.to_csv(f"data/{provider}/site_avg.csv")


def process_data():
    charging_units = get_charging_unit_data()

    obs_results = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                fetch_charger_data, cu, obs_id, DAYS_BACK_IN_TIME, PROVIDER, TABLE_NAME
            )
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
    print("Data fetched")
    # Merge data
    complete_data = merge_data(charging_units, obs_results)
    if not complete_data:
        print("No valid observation data found.")
        return
    print("Data merged")
    df = pd.DataFrame([d.__dict__ for d in complete_data])
    save_data(df, charging_units, PROVIDER)


if __name__ == "__main__":
    process_data()
