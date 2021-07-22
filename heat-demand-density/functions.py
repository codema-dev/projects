from collections import defaultdict
from pathlib import Path

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


def drop_small_areas_not_in_boundaries(
    bers: pd.DataFrame, small_area_boundaries: gpd.GeoDataFrame
) -> pd.DataFrame:
    small_areas = small_area_boundaries["small_area"].to_numpy()
    return bers.query("small_area in @small_areas")


def amalgamate_heat_demands_to_small_areas(
    residential: pd.DataFrame, non_residential: pd.DataFrame
) -> pd.DataFrame:
    residential_small_areas = (
        residential.groupby("small_area")["heat_demand_mwh_per_y"]
        .sum()
        .rename("residential_heat_demand_mwh_per_y")
    )
    index = residential_small_areas.index
    non_residential_small_areas = (
        non_residential.groupby("small_area")["heat_demand_mwh_per_y"]
        .sum()
        .reindex(index)
        .fillna(0)
        .rename("non_residential_heat_demand_mwh_per_y")
    )
    return pd.concat(
        [residential_small_areas, non_residential_small_areas], axis="columns"
    )


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
    demand_tj_per_y = mwh_to_tj * demand
    return (
        demand_tj_per_y.divide(polygon_area_km2_by_small_area, axis="rows")
        .rename(columns=lambda x: x.replace("_mwh_per_y", "_tj_per_km2y"))
        .assign(total_heat_demand_tj_per_km2y=lambda df: df.sum(axis="columns"))
    )


def link_demands_to_boundaries(
    demands: pd.DataFrame, boundaries: gpd.GeoDataFrame
) -> None:
    return boundaries.merge(demands, left_on="small_area", right_index=True, how="left")


def save_demand_map(demand_map: gpd.GeoDataFrame, filepath: Path) -> None:
    demand_map.to_file(filepath, driver="GeoJSON")
