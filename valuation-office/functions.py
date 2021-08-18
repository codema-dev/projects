from collections import defaultdict
from pathlib import Path
from zipfile import ZipFile

import fsspec
import geopandas as gpd
import pandas as pd


def read_csv(url: str) -> pd.DataFrame:
    with fsspec.open(url) as f:
        return pd.read_csv(f)


def read_parquet(url: str) -> pd.DataFrame:
    with fsspec.open(url) as f:
        return pd.read_parquet(f)


def read_geoparquet(url: str) -> pd.DataFrame:
    with fsspec.open(url) as f:
        return gpd.read_parquet(f)


def read_file(url: str) -> pd.DataFrame:
    with fsspec.open(url) as f:
        return gpd.read_file(f)


def read_benchmark_uses(url: str, filesystem_name: str) -> pd.DataFrame:
    fs = fsspec.filesystem(filesystem_name)
    uses_grouped_by_category = defaultdict()
    with ZipFile(fs.open(url)) as zf:
        for filename in zf.namelist():
            name = filename.split("/")[-1].replace(".txt", "")
            with zf.open(filename, "r") as f:
                uses_grouped_by_category[name] = [
                    line.rstrip().decode("utf-8") for line in f
                ]
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


def link_valuation_office_to_benchmarks(
    valuation_office: pd.DataFrame,
    benchmark_uses: pd.DataFrame,
    benchmarks: pd.DataFrame,
    valuation_office_use_column: str,
    use_column: str,
    benchmark_column: str,
) -> pd.Series:
    valuation_office[use_column] = valuation_office[valuation_office_use_column].map(
        benchmark_uses
    )
    return valuation_office.merge(
        benchmarks, left_on=use_column, right_on=benchmark_column, how="left"
    )


def apply_benchmarks_to_valuation_office_floor_areas(
    valuation_office: pd.DataFrame,
    demand_column: str,
    energy_benchmark_column: str,
    floor_area_column: str,
) -> pd.Series:
    valuation_office[demand_column] = (
        valuation_office[energy_benchmark_column] * valuation_office[floor_area_column]
    )
    return valuation_office


def estimate_heat_demand(
    valuation_office: pd.DataFrame,
    heat_demand_column: str,
    fossil_fuel_benchmark_column: str,
    floor_area_column: str,
    industrial_space_heat_column: str,
    assumed_boiler_efficiency: float,
) -> pd.DataFrame:
    non_industrial_heat_demand_kwh_per_y = (
        valuation_office[fossil_fuel_benchmark_column]
        * valuation_office[floor_area_column]
    ) / assumed_boiler_efficiency
    industrial_heat_demand_kwh_per_y = (
        valuation_office[industrial_space_heat_column]
        * valuation_office[floor_area_column]
    )
    kwh_to_mwh = 1e-3
    valuation_office[heat_demand_column] = (
        non_industrial_heat_demand_kwh_per_y.fillna(0)
        + industrial_heat_demand_kwh_per_y.fillna(0)
    ) * kwh_to_mwh
    return valuation_office


def to_csv(df: pd.DataFrame, filepath: Path) -> None:
    df.to_csv(filepath)
