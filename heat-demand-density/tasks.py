from collections import defaultdict
from configparser import ConfigParser
from io import BytesIO
from pathlib import Path
from typing import Callable
from typing import List
from zipfile import ZipFile
import zipfile

import fsspec
import geopandas as gpd
import pandas as pd


def read_excel(url: str) -> pd.DataFrame:
    with fsspec.open(url) as f:
        return pd.read_excel(f)


def read_parquet(url: str) -> pd.DataFrame:
    with fsspec.open(url) as f:
        return pd.read_parquet(f)


def read_geoparquet(url: str) -> pd.DataFrame:
    with fsspec.open(url) as f:
        return gpd.read_parquet(f)


def read_zipped_shp(url: str) -> pd.DataFrame:
    with fsspec.open(url) as f:
        return gpd.read_file(f)


def read_benchmark_uses(url: str, filesystem_name: str) -> pd.DataFrame:
    fs = fsspec.filesystem(filesystem_name)
    uses_grouped_by_category = defaultdict()
    for file in fs.glob(url + "/*.txt"):
        name = file.split("/")[-1].replace(".txt", "")
        with fs.open(file, "r") as f:
            uses_grouped_by_category[name] = [line.rstrip() for line in f]
    return {i: k for k, v in uses_grouped_by_category.items() for i in v}


def link_valuation_office_to_small_areas(
    valuation_office: pd.DataFrame, small_area_boundaries: gpd.GeoDataFrame
) -> pd.DataFrame:
    valuation_office_map = gpd.GeoDataFrame(
        valuation_office,
        geometry=gpd.points_from_xy(
            x=valuation_office["X_ITM"], y=valuation_office["Y_ITM"], crs="EPSG:2157"
        ),
    )
    return gpd.sjoin(
        valuation_office_map, small_area_boundaries.to_crs(epsg=2157), op="within"
    ).drop(columns="index_right")


def apply_benchmarks_to_valuation_office_floor_areas(
    valuation_office: pd.DataFrame,
    benchmark_uses: pd.DataFrame,
    benchmarks: pd.DataFrame,
    assumed_boiler_efficiency: float,
) -> pd.Series:
    valuation_office["use"] = valuation_office["Use1"].map(benchmark_uses)
    with_benchmarks = valuation_office.merge(
        benchmarks, left_on="use", right_on="Benchmark", how="left", indicator=True
    )
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
    return with_benchmarks[["small_area", "heat_demand_mwh_per_y"]]


def extract_residential_heat_demand(bers: pd.DataFrame) -> pd.Series:
    kwh_to_mwh = 1e-3
    bers["heat_demand_mwh_per_y"] = (
        bers["main_sh_demand"]
        + bers["suppl_sh_demand"]
        + bers["main_hw_demand"]
        + bers["suppl_hw_demand"]
    ) * kwh_to_mwh
    return bers[["small_area", "heat_demand_mwh_per_y"]]


def amalgamate_heat_demands_to_small_areas(
    residential: pd.DataFrame, non_residential: pd.DataFrame
) -> pd.DataFrame:
    residential_small_areas = residential.groupby("small_area")[
        "heat_demand_mwh_per_y"
    ].sum()
    index = residential_small_areas.index
    non_residential_small_areas = (
        non_residential.groupby("small_area")["heat_demand_mwh_per_y"]
        .sum()
        .reindex(index)
        .fillna(0)
    )
    return (residential_small_areas + non_residential_small_areas).to_frame()


def convert_from_mwh_per_y_to_tj_per_km2(
    demand: pd.DataFrame, small_area_boundaries: gpd.GeoDataFrame
) -> pd.DataFrame:
    index = demand.index
    m2_to_km2 = 1e-6
    small_area_boundaries["polygon_area_km2"] = (
        small_area_boundaries.geometry.area * m2_to_km2
    )
    polygon_area_km2_by_small_area = (
        small_area_boundaries[["small_area", "polygon_area_km2"]]
        .set_index("small_area")
        .squeeze()
        .reindex(index)
    )
    mwh_to_tj = 0.0036
    demand_tj_per_y = (mwh_to_tj * demand).squeeze()
    return (
        (demand_tj_per_y / polygon_area_km2_by_small_area)
        .rename("heat_demand_tj_per_km2y")
        .to_frame()
    )
