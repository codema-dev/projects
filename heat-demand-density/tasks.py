from collections import defaultdict
from configparser import ConfigParser
from pathlib import Path
from typing import Callable
from typing import List

import fsspec
import pandas as pd


def load_valuation_office(urls: List[Path], filesystem_name: str) -> pd.DataFrame:
    dfs: List[pd.DataFrame] = []
    fs = fsspec.filesystem(filesystem_name)
    for url in urls:
        with fs.open(url) as f:
            df = pd.read_excel(url)
        dfs.append(df)
    return pd.concat(dfs).reset_index(drop=True)


def load_bers(url: str, filesystem_name: str) -> pd.DataFrame:
    fs = fsspec.filesystem(filesystem_name)
    with fs.open(url) as f:
        return pd.read_parquet(url)


def load_benchmark_uses(url: str, filesystem_name: str) -> pd.DataFrame:
    fs = fsspec.filesystem(filesystem_name)
    uses_grouped_by_category = defaultdict()
    for file in fs.glob(url + "/*.txt"):
        name = file.split("/")[-1].replace(".txt", "")
        with fs.open(file, "r") as f:
            uses_grouped_by_category[name] = [line.rstrip() for line in f]
    return {i: k for k, v in uses_grouped_by_category.items() for i in v}


def load_benchmarks(url: str, filesystem_name: str) -> pd.DataFrame:
    fs = fsspec.filesystem(filesystem_name)
    with fs.open(url) as f:
        return pd.read_excel(f)


def apply_benchmarks_to_valuation_office_floor_areas(
    valuation_office: pd.DataFrame,
    benchmark_uses: pd.DataFrame,
    benchmarks: pd.DataFrame,
) -> pd.DataFrame:
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
