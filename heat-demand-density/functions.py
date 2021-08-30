from collections import defaultdict
from pathlib import Path
from typing import Any
from typing import Dict

import jupytext
from jupytext import kernels
from jupytext import header
import fsspec
import geopandas as gpd
import pandas as pd
from prefect.tasks.jupyter import ExecuteNotebook


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


def link_small_areas_to_local_authorities(
    small_area_boundaries: gpd.GeoDataFrame,
    local_authority_boundaries: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    small_area_points = small_area_boundaries.copy()
    small_area_points[
        "geometry"
    ] = small_area_boundaries.geometry.representative_point()

    # NOTE: fiona infers an incorrect encoding for the local authority boundaries
    small_areas_in_local_authorities = (
        gpd.sjoin(
            small_area_points,
            local_authority_boundaries[["COUNTYNAME", "geometry"]],
            op="within",
        )
        .drop(columns="index_right")
        .rename(columns={"COUNTYNAME": "local_authority"})
        .assign(local_authority=lambda df: df["local_authority"].str.decode("latin-1"))
    )
    return small_area_boundaries.merge(
        small_areas_in_local_authorities.drop(columns="geometry")
    )


def apply_benchmarks_to_valuation_office_floor_areas(
    valuation_office: pd.DataFrame,
    benchmark_uses: pd.DataFrame,
    benchmarks: pd.DataFrame,
    assumed_boiler_efficiency: float,
) -> pd.Series:

    # link to benchmarks based on building use
    valuation_office["use"] = valuation_office["Use1"].map(benchmark_uses)
    with_benchmarks = valuation_office.merge(
        benchmarks, left_on="use", right_on="Benchmark", how="left", indicator=True
    )

    # replace unexpectedly large floor areas with a typical area
    with_benchmarks["bounded_area_m2"] = with_benchmarks["Total_SQM"]
    where_area_greater_than_expected = (
        with_benchmarks["Total_SQM"] > with_benchmarks["Area Upper Bound [m²]"]
    )
    with_benchmarks.loc[
        where_area_greater_than_expected, "bounded_area_m2"
    ] = with_benchmarks.loc[where_area_greater_than_expected, "Typical Area [m²]"]

    # calculate heat demand
    non_industrial_heat_demand_kwh_per_y = (
        with_benchmarks["Typical fossil fuel [kWh/m²y]"]
        * with_benchmarks["bounded_area_m2"]
        * with_benchmarks["% Suitable for DH or HP"]
    ) / assumed_boiler_efficiency
    industrial_heat_demand_kwh_per_y = (
        with_benchmarks["Industrial space heat [kWh/m²y]"]
        * with_benchmarks["bounded_area_m2"]
        * with_benchmarks["% Suitable for DH or HP"]
    )
    kwh_to_mwh = 1e-3
    with_benchmarks["heat_demand_mwh_per_y"] = (
        non_industrial_heat_demand_kwh_per_y.fillna(0)
        + industrial_heat_demand_kwh_per_y.fillna(0)
    ) * kwh_to_mwh

    return with_benchmarks


def extract_residential_heat_demand(bers: pd.DataFrame) -> pd.Series:
    kwh_to_mwh = 1e-3
    bers["heat_demand_mwh_per_y"] = (
        bers["main_sh_demand"]
        + bers["suppl_sh_demand"]
        + bers["main_hw_demand"]
        + bers["suppl_hw_demand"]
    ) * kwh_to_mwh
    return bers


def amalgamate_heat_demands_to_small_areas(
    residential: pd.DataFrame, non_residential: pd.DataFrame
) -> pd.DataFrame:
    residential_small_area_demand = (
        residential.groupby("small_area")["heat_demand_mwh_per_y"]
        .sum()
        .rename("residential_heat_demand_mwh_per_y")
    )
    residential_small_area_count = (
        residential.groupby("small_area")["heat_demand_mwh_per_y"]
        .size()
        .rename("number_of_residential_buildings")
    )
    index = residential_small_area_demand.index
    non_residential_small_area_demand = (
        non_residential.groupby("small_area")["heat_demand_mwh_per_y"]
        .sum()
        .reindex(index)
        .fillna(0)
        .rename("non_residential_heat_demand_mwh_per_y")
    )
    non_residential_small_area_count = (
        non_residential.groupby("small_area")["heat_demand_mwh_per_y"]
        .size()
        .reindex(index)
        .fillna(0)
        .rename("number_of_non_residential_buildings")
    )
    return pd.concat(
        [
            residential_small_area_demand,
            non_residential_small_area_demand,
            residential_small_area_count,
            non_residential_small_area_count,
        ],
        axis="columns",
    )


def convert_from_mwh_per_y_to_tj_per_km2(
    demand: pd.DataFrame, small_area_boundaries: gpd.GeoDataFrame
) -> pd.DataFrame:
    index = demand.index
    m2_to_km2 = 1e-6
    polygon_area_km2 = (small_area_boundaries.geometry.area * m2_to_km2).reindex(index)
    mwh_to_tj = 0.0036
    demand["polygon_area_km2"] = polygon_area_km2
    demand["residential_heat_demand_tj_per_km2y"] = (
        demand["residential_heat_demand_mwh_per_y"]
        .multiply(mwh_to_tj)
        .divide(polygon_area_km2, axis="rows")
    )
    demand["non_residential_heat_demand_tj_per_km2y"] = (
        demand["non_residential_heat_demand_mwh_per_y"]
        .multiply(mwh_to_tj)
        .divide(polygon_area_km2, axis="rows")
    )
    return demand.dropna(how="any")


def link_demands_to_boundaries(
    demands: pd.DataFrame, boundaries: gpd.GeoDataFrame
) -> None:
    return boundaries.merge(
        demands, left_on="small_area", right_index=True, how="right"
    )


def save_to_geojson(demand_map: gpd.GeoDataFrame, filepath: Path) -> None:
    demand_map.to_file(filepath, driver="GeoJSON")


def _convert_py_to_ipynb(
    input_filepath: Path,
    output_filepath: Path,
) -> None:
    notebook = jupytext.read(input_filepath)
    notebook["metadata"]["kernelspec"] = kernels.kernelspec_from_language("python")
    jupytext.write(notebook, output_filepath, fmt="py:light")


def execute_python_file(
    py_filepath: Path,
    ipynb_filepath: Path,
    parameters: Dict[str, Any],
) -> None:
    _convert_py_to_ipynb(py_filepath, ipynb_filepath)
    exe = ExecuteNotebook()
    exe.run(path=ipynb_filepath, parameters=parameters)
