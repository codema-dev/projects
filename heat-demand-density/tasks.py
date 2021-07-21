from collections import defaultdict
from configparser import ConfigParser
from pathlib import Path
from typing import Callable
from typing import List

import fsspec
import geopandas as gpd
import pandas as pd


def read_parquet(url: str, filesystem_name: str) -> pd.DataFrame:
    fs = fsspec.filesystem(filesystem_name)
    with fs.open(url) as f:
        return pd.read_parquet(url)


def read_benchmark_uses(url: str, filesystem_name: str) -> pd.DataFrame:
    fs = fsspec.filesystem(filesystem_name)
    uses_grouped_by_category = defaultdict()
    for file in fs.glob(url + "/*.txt"):
        name = file.split("/")[-1].replace(".txt", "")
        with fs.open(file, "r") as f:
            uses_grouped_by_category[name] = [line.rstrip() for line in f]
    return {i: k for k, v in uses_grouped_by_category.items() for i in v}


def read_excel(url: str, filesystem_name: str) -> pd.DataFrame:
    fs = fsspec.filesystem(filesystem_name)
    with fs.open(url) as f:
        return pd.read_excel(f)


def link_valuation_office_to_small_areas(
    valuation_office: pd.DataFrame, small_area_boundaries: gpd.GeoDataFrame
) -> pd.DataFrame:
    pass


def apply_benchmarks_to_valuation_office_floor_areas(
    valuation_office: pd.DataFrame,
    benchmark_uses: pd.DataFrame,
    benchmarks: pd.DataFrame,
) -> pd.Series:
    valuation_office["use"] = valuation_office["Use1"].map(benchmark_uses)
    with_benchmarks = valuation_office.merge(
        benchmarks, left_on="use", right_on="Benchmark", how="left", indicator=True
    )
    assumed_boiler_efficiency = 0.9
    non_industrial_heat_demand_kwh_per_y = (
        with_benchmarks["Typical fossil fuel [kWh/m²y]"] * with_benchmarks["Total_SQM"]
    ) / assumed_boiler_efficiency
    industrial_heat_demand_kwh_per_y = (
        with_benchmarks["Industrial space heat [kWh/m²y]"]
        * with_benchmarks["Total_SQM"]
    )
    kwh_to_mwh = 1e-3
    with_benchmarks["heat_demand_mwh_per_y"] = (
        non_industrial_heat_demand_kwh_per_y.fillna(0)
        + industrial_heat_demand_kwh_per_y.fillna(0)
    ) * kwh_to_mwh
    return with_benchmarks[["PropertyNo", "Use1", "Benchmark", "heat_demand_mwh_per_y"]]
