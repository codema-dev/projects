from collections import defaultdict
from pathlib import Path

import jupytext
from jupytext import kernels
from jupytext import header
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


def extract_non_residential_emissions(
    valuation_office: pd.DataFrame,
    benchmark_uses: pd.DataFrame,
    benchmarks: pd.DataFrame,
) -> pd.Series:
    kwh_gas_to_tco2 = 204.7e-6
    kwh_electricity_to_tco2 = 295.1e-6

    valuation_office["use"] = valuation_office["Use1"].map(benchmark_uses)
    with_benchmarks = valuation_office.merge(
        benchmarks, left_on="use", right_on="Benchmark", how="left", indicator=True
    )

    non_industrial_tco2_per_y = (
        with_benchmarks["Typical fossil fuel [kWh/m²y]"].fillna(0)
        * with_benchmarks["Total_SQM"]
        * kwh_gas_to_tco2
        + with_benchmarks["Typical electricity [kWh/m²y]"].fillna(0)
        * with_benchmarks["Total_SQM"]
        * kwh_electricity_to_tco2
    )

    industrial_tco2_per_y = (
        (
            with_benchmarks["Industrial building total [kWh/m²y]"].fillna(0)
            + with_benchmarks["Industrial process energy [kWh/m²y]"].fillna(0)
        )
        * with_benchmarks["Total_SQM"]
        * kwh_gas_to_tco2
    )

    with_benchmarks["emissions_tco2_per_y"] = (
        non_industrial_tco2_per_y + industrial_tco2_per_y
    )

    return with_benchmarks[["small_area", "Benchmark", "emissions_tco2_per_y"]]


def extract_residential_emissions(bers: pd.DataFrame) -> pd.Series:
    kwh_electricity_to_tco2 = 295.1e-6
    emission_factors = bers["main_sh_boiler_fuel"].map(
        {
            "Mains Gas": 204.7e-6,
            "Heating Oil": 263e-6,
            "Electricity": kwh_electricity_to_tco2,
            "Bulk LPG": 229e-6,
            "Wood Pellets (bags)": 390e-6,
            "Wood Pellets (bulk)": 160e-6,
            "Solid Multi-Fuel": 390e-6,
            "Manuf.Smokeless Fuel": 390e-6,
            "Bottled LPG": 229e-6,
            "House Coal": 340e-6,
            "Wood Logs": 390e-6,
            "Peat Briquettes": 355e-6,
            "Anthracite": 340e-6,
        }
    )
    heating_demand = (
        bers["main_sh_demand"]
        + bers["suppl_sh_demand"]
        + bers["main_hw_demand"]
        + bers["suppl_hw_demand"]
    )
    electricity_demand = (bers["pump_fan_demand"] + bers["lighting_demand"]) * 2
    bers["emissions_tco2_per_y"] = (
        heating_demand * emission_factors + electricity_demand * kwh_electricity_to_tco2
    )

    return bers[["small_area", "emissions_tco2_per_y"]]


def drop_small_areas_not_in_boundaries(
    bers: pd.DataFrame, small_area_boundaries: gpd.GeoDataFrame
) -> pd.DataFrame:
    small_areas = small_area_boundaries["small_area"].to_numpy()
    return bers.query("small_area in @small_areas")


def amalgamate_emissions_to_small_areas(
    residential: pd.DataFrame, non_residential: pd.DataFrame
) -> pd.DataFrame:
    residential_small_areas = (
        residential.groupby("small_area")["emissions_tco2_per_y"]
        .sum()
        .rename("residential_emissions_tco2_per_y")
    )
    index = residential_small_areas.index
    non_residential_small_areas = (
        non_residential.groupby("small_area")["emissions_tco2_per_y"]
        .sum()
        .reindex(index)
        .fillna(0)
        .rename("non_residential_emissions_tco2_per_y")
    )
    return pd.concat(
        [residential_small_areas, non_residential_small_areas], axis="columns"
    )


def link_emissions_to_boundaries(
    demands: pd.DataFrame, boundaries: gpd.GeoDataFrame
) -> None:
    return boundaries.merge(demands, left_on="small_area", right_index=True, how="left")


def save_demand_map(demand_map: gpd.GeoDataFrame, filepath: Path) -> None:
    demand_map.to_file(filepath, driver="GeoJSON")


def convert_file_to_ipynb(
    input_filepath: Path,
    output_filepath: Path,
    fmt: str,
    language: str = "python",
) -> None:
    notebook = jupytext.read(input_filepath)
    notebook["metadata"]["kernelspec"] = kernels.kernelspec_from_language(language)
    jupytext.write(notebook, output_filepath, fmt=fmt)
