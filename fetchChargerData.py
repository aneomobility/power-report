import calendar
from datetime import datetime, timedelta
import os
import pandas as pd
from azure.data.tables import TableClient
from azure.core.credentials import AzureNamedKeyCredential
import csv
import concurrent.futures
import psycopg
from dataclasses import dataclass
from typing import Optional
import plotly.graph_objects as go
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DB_URL")
AZ_ACCOUNT = os.getenv("AZ_ACCOUNT")
AZ_KEY = os.getenv("AZ_KEY")


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


def get_charging_unit_data():
    data = []
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT
                    "PulseStructureSite".id AS "siteId",
                    "PulseStructureSite"."siteKey",
                    "PulseStructureSite"."name",
                    "PulseStructureSite"."fuseRating" AS "siteFuseRating (A)",
                    "PulseStructureSite"."netType",
                    REGEXP_REPLACE("PulseStructureSite"."netType", '[^0-9]', '', 'g')::INTEGER AS "net (V)",
                    REGEXP_REPLACE("PulseStructureSite"."netType", '[^0-9]', '', 'g')::INTEGER * "PulseStructureSite"."fuseRating" * SQRT(3) / 1000 AS "sitePower (kW)",
                    "PulseStructureSite"."hasLoadbalancer",
                    "PulseStructureCircuit".id AS "circuitId",
                    "PulseStructureCircuit"."name" AS "circuitName",
                    "PulseStructureCircuit"."fuseRating" AS "circuitFuseRating (A)",
                    "PulseStructureChargingUnit".provider,
                    "PulseStructureChargingUnit"."chargerId",
                    REGEXP_REPLACE("PulseStructureSite"."netType", '[^0-9]', '', 'g')::INTEGER * "PulseStructureCircuit"."fuseRating" * SQRT(3) / 1000 AS "circuitPower (kW)"
                FROM
                    "PulseStructureSite"
                    LEFT JOIN "PulseStructureCircuit" ON "PulseStructureSite".id = "PulseStructureCircuit"."siteId"
                    LEFT JOIN "PulseStructureChargingUnit" ON "PulseStructureCircuit".id = "PulseStructureChargingUnit"."circuitId"
                WHERE
                    "PulseStructureChargingUnit".provider = 'EASEE'
                    AND "PulseStructureSite"."name" LIKE 'Prestestien Brl%'
                """
            )

            for record in cur:
                charging_unit = ChargingUnit(
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
                data.append(charging_unit)
    return data


def fetch_charger_data(d, obsId, days_back_in_time):
    credential = AzureNamedKeyCredential(AZ_ACCOUNT, AZ_KEY)

    now = datetime.now()
    days_back_in_time = now - timedelta(days_back_in_time)

    now_iso = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    one_week_ago_iso = days_back_in_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    if d.provider != "EASEE":
        return []
    chargerId = d.charger_id
    my_filter = f"PartitionKey eq 'EASEE_{obsId}' and RowKey gt '{chargerId}_{one_week_ago_iso}' and RowKey lt '{chargerId}_{now_iso}'"
    table_client = TableClient(
        credential=credential,
        endpoint="https://pulsehubstoraged998526f.table.core.windows.net",
        table_name="formattedobservations",
    )
    entities = table_client.query_entities(my_filter)
    charger_data = [
        ObsData(
            charger_id=chargerId,
            timestamp=entity["pulseTimestamp"],
            value=entity["value"],
            observation_id=entity["observationId"],
        )
        for entity in entities
    ]
    charger_data.sort(key=lambda x: x.timestamp)
    return charger_data


def write_to_csv(file_path, data_instances):
    with open(file_path, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(data_instances[0].__annotations__.keys())
        for instance in data_instances:
            writer.writerow(instance.__dict__.values())


def merge_data(data, results):
    complete_data = []
    for result in results:
        if result.value > 0:
            charging_unit = next(
                (x for x in data if x.charger_id == result.charger_id), None
            )
            complete = ChargingUnitComplete(
                charger_id=result.charger_id,
                timestamp=result.timestamp,
                value_kWh=result.value,
                observation_id=result.observation_id,
                circuit_fuse_rating_a=charging_unit.circuit_fuse_rating_a,
                circuit_id=charging_unit.circuit_id,
                circuit_name=charging_unit.circuit_name,
                has_loadbalancer=charging_unit.has_loadbalancer,
                name=charging_unit.name,
                net_type=charging_unit.net_type,
                net_v=charging_unit.net_v,
                provider=charging_unit.provider,
                site_fuse_rating_a=charging_unit.site_fuse_rating_a,
                site_id=charging_unit.site_id,
                site_key=charging_unit.site_key,
                site_power_kw=charging_unit.site_power_kw,
                circuit_power_kw=charging_unit.circuit_power_kw,
            )
            complete_data.append(complete)
    return complete_data


if __name__ == "__main__":
    obsIds = ["122"]
    days_back_in_time = 30
    data = get_charging_unit_data()
    results = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(fetch_charger_data, d, obsId, days_back_in_time)
            for d in data
            for obsId in obsIds
        ]
        for future in concurrent.futures.as_completed(futures):
            results.extend(future.result())

    if results:
        complete_data = merge_data(data, results)

        df = pd.DataFrame([d.__dict__ for d in complete_data])
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        df["value_kWh"] = pd.to_numeric(df["value_kWh"])
        df["circuit_power_kw"] = pd.to_numeric(df["circuit_power_kw"])

        site_names = df[["site_key", "name"]].drop_duplicates()

        report = (
            df.groupby(["site_key", "name", "circuit_id", "timestamp"])
            .agg(sum_value_kWh=("value_kWh", "sum"), chargers=("value_kWh", "count"))
            .reset_index()
        )

        highest_peaks_avg = (
            report.groupby("site_key")
            .apply(lambda x: x.nlargest(3, "sum_value_kWh")["sum_value_kWh"].mean())
            .reset_index(name="avg_three_highest_peaks")
        ).sort_values("avg_three_highest_peaks", ascending=False)

        highest_peaks_avg.to_csv("highest_peaks_avg.csv", index=False)

        # select 10 highest peak sites and filter report to only include those
        highest_peaks = highest_peaks_avg.nlargest(10, "avg_three_highest_peaks")[
            "site_key"
        ]
        report = report[report["site_key"].isin(highest_peaks)]

        report["week_of_day"] = report["timestamp"].dt.dayofweek
        report["hour"] = report["timestamp"].dt.hour.astype(int)
        report = (
            report.groupby(["site_key", "name", "circuit_id", "week_of_day", "hour"])
            .agg(sum_value_kWh=("sum_value_kWh", "mean"), chargers=("chargers", "mean"))
            .reset_index()
        )

        report = report.merge(
            df[["site_key", "circuit_id", "circuit_power_kw"]].drop_duplicates(),
            on=["site_key", "circuit_id"],
            how="left",
        )

        report["used_power_ratio"] = (
            report["sum_value_kWh"] / report["circuit_power_kw"] * 100
        ).round(2)

        expanded_data = []

        for site, site_data in report.groupby("site_key"):
            for circuit in site_data["circuit_id"].unique():
                circuit_data = site_data[site_data["circuit_id"] == circuit]
                all_combinations = pd.MultiIndex.from_product(
                    [
                        [site],
                        [circuit],
                        range(7),
                        range(24),
                    ],
                    names=["site_key", "circuit_id", "week_of_day", "hour"],
                ).to_frame(index=False)

                merged = (
                    pd.merge(
                        all_combinations,
                        circuit_data,
                        on=["site_key", "circuit_id", "week_of_day", "hour"],
                        how="left",
                    )
                    .fillna({"sum_value_kWh": 0})
                    .fillna({"circuit_power_kw": 0})
                    .fillna({"used_power_ratio": 0})
                )
                expanded_data.append(merged)

        report_expanded = pd.concat(expanded_data, ignore_index=True).sort_values(
            by=["site_key", "circuit_id", "week_of_day", "hour"]
        )

        fig = go.Figure()
        for site in report_expanded["site_key"].unique():
            site_data = report_expanded[report_expanded["site_key"] == site]
            fig.add_trace(
                go.Bar(
                    x=[],
                    y=[],
                    name=f"Site {site}",
                    legendgroup=f"site_{site}",
                    showlegend=True,
                )
            )
            site_name = site_names[site_names["site_key"] == site]["name"].values[0]

            for circuit in site_data["circuit_id"].unique():
                circuit_data = site_data[site_data["circuit_id"] == circuit]
                fig.add_trace(
                    go.Bar(
                        x=[
                            [
                                calendar.day_name[day]
                                for day in circuit_data["week_of_day"]
                            ],
                            [
                                str(hour) if hour > 9 else f"0{hour}"
                                for hour in circuit_data["hour"]
                            ],
                        ],
                        y=circuit_data["sum_value_kWh"],
                        name=f"Circuit {circuit}",
                        legendgroup=f"site_{site}",
                        legendgrouptitle_text=f"Site {site_name}: {highest_peaks_avg[highest_peaks_avg['site_key'] == site]['avg_three_highest_peaks'].values[0]:.2f} kWh",
                        showlegend=True,
                        text=circuit_data["chargers"],
                        textposition="outside",
                        textfont=dict(size=52),
                    )
                )

        fig.update_layout(
            legend=dict(groupclick="toggleitem"),
            barmode="group",
            yaxis_title="ΣEnergy (kWh)",
        )

        fig.show()
        if True:
            fig.write_html("index.html")

        if False:
            report_expanded.to_csv("report.csv", index=False)
    else:
        print("No data found")
