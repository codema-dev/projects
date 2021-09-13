from collections import defaultdict
import json
from typing import Any
from zipfile import ZipFile

import pandas as pd


def convert_benchmark_uses_to_json(upstream: Any, product: Any) -> None:
    uses_grouped_by_category = defaultdict()
    with ZipFile(upstream["download_benchmark_uses"]) as zf:
        for filename in zf.namelist():
            name = filename.split("/")[-1].replace(".txt", "")
            with zf.open(filename, "r") as f:
                uses_grouped_by_category[name] = [
                    line.rstrip().decode("utf-8") for line in f
                ]
    benchmark_uses = {i: k for k, v in uses_grouped_by_category.items() for i in v}
    with open(product, "w") as f:
        json.dump(benchmark_uses, f)


def weather_adjust_benchmarks(upstream: Any, product: Any) -> None:

    benchmarks = pd.read_csv(upstream["download_benchmarks"])

    # 5y average for Dublin Airport from 2015 to 2020
    dublin_degree_days = 2175
    tm46_degree_days = 2021
    degree_day_factor = dublin_degree_days / tm46_degree_days

    fossil_fuel = benchmarks["Typical fossil fuel [kWh/m²y]"] * benchmarks[
        "% fossil fuel pro-rated to degree days"
    ] * degree_day_factor + benchmarks["Typical fossil fuel [kWh/m²y]"] * (
        1 - benchmarks["% fossil fuel pro-rated to degree days"]
    )
    fossil_fuel_heat = fossil_fuel * benchmarks["% suitable for DH or HP"]

    industrial_heat = (
        benchmarks["Industrial space heat [kWh/m²y]"] * degree_day_factor
        + benchmarks["Industrial process energy [kWh/m²y]"]
        * benchmarks["% suitable for DH or HP"]
    )

    normalised_benchmarks = pd.DataFrame(
        {
            "Benchmark": benchmarks["Benchmark"],
            "typical_area_m2": benchmarks["Typical Area [m²]"],
            "area_upper_bound_m2": benchmarks["Area Upper Bound [m²]"],
            "typical_electricity_kwh_per_m2y": benchmarks[
                "Typical electricity [kWh/m²y]"
            ],
            "typical_fossil_fuel_kwh_per_m2y": fossil_fuel,
            "typical_building_energy_kwh_per_m2y": benchmarks[
                "Industrial building total [kWh/m²y]"
            ],
            "typical_process_energy_kwh_per_m2y": benchmarks[
                "Industrial process energy [kWh/m²y]"
            ],
            "typical_fossil_fuel_heat_kwh_per_m2y": fossil_fuel_heat,
            "industrial_heat_kwh_per_m2y": industrial_heat,
        }
    )

    normalised_benchmarks.to_csv(product)


def link_valuation_office_to_benchmarks(upstream: Any, product: Any) -> None:
    buildings = pd.read_csv(upstream["download_buildings"])
    benchmarks = pd.read_csv(upstream["weather_adjust_benchmarks"], index_col=0)
    with open(upstream["convert_benchmark_uses_to_json"], "r") as f:
        benchmark_uses = json.load(f)

    benchmarks = buildings["Use1"].map(benchmark_uses).rename("Benchmark")
    propertyno_benchmark_map = pd.concat([buildings["PropertyNo"], benchmarks], axis=1)

    propertyno_benchmark_map.to_csv(product, index=False)
