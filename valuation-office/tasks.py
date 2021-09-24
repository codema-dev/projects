from collections import defaultdict
import json
from typing import Any
from zipfile import ZipFile

import geopandas as gpd
import pandera
from pandera import DataFrameSchema, Column, Check, Index
import pandas as pd


def concatenate_local_authority_floor_areas(upstream: Any, product: Any) -> None:
    dcc = pd.read_excel(upstream["download_valuation_office_floor_areas_dcc"])
    dlrcc = pd.read_excel(upstream["download_valuation_office_floor_areas_dlrcc"])
    sdcc = pd.read_excel(upstream["download_valuation_office_floor_areas_sdcc"])
    fcc = pd.read_excel(upstream["download_valuation_office_floor_areas_fcc"])
    dublin = pd.concat([dcc, dlrcc, sdcc, fcc])
    dublin.to_csv(product, index=False)


def validate_dublin_floor_areas(product: Any) -> None:
    dublin_floor_areas = pd.read_csv(product)
    schema = DataFrameSchema(
        columns={
            "PropertyNo": Column(
                dtype=pandera.engines.numpy_engine.Int64,
                checks=[
                    Check.greater_than_or_equal_to(min_value=272845.0),
                    Check.less_than_or_equal_to(max_value=5023334.0),
                ],
                nullable=False,
                unique=False,
                coerce=False,
                required=True,
                regex=False,
            ),
            "County": Column(
                dtype=pandera.engines.numpy_engine.Object,
                checks=None,
                nullable=False,
                unique=False,
                coerce=False,
                required=True,
                regex=False,
            ),
            "LA": Column(
                dtype=pandera.engines.numpy_engine.Object,
                checks=None,
                nullable=False,
                unique=False,
                coerce=False,
                required=True,
                regex=False,
            ),
            "Category": Column(
                dtype=pandera.engines.numpy_engine.Object,
                checks=None,
                nullable=False,
                unique=False,
                coerce=False,
                required=True,
                regex=False,
            ),
            "Use1": Column(
                dtype=pandera.engines.numpy_engine.Object,
                checks=None,
                nullable=True,
                unique=False,
                coerce=False,
                required=True,
                regex=False,
            ),
            "Use2": Column(
                dtype=pandera.engines.numpy_engine.Object,
                checks=None,
                nullable=True,
                unique=False,
                coerce=False,
                required=True,
                regex=False,
            ),
            "List_Status": Column(
                dtype=pandera.engines.numpy_engine.Object,
                checks=None,
                nullable=False,
                unique=False,
                coerce=False,
                required=True,
                regex=False,
            ),
            "Total_SQM": Column(
                dtype=pandera.engines.numpy_engine.Float64,
                checks=[
                    Check.greater_than_or_equal_to(min_value=0.0),
                    Check.less_than_or_equal_to(max_value=5373112.83),
                ],
                nullable=False,
                unique=False,
                coerce=False,
                required=True,
                regex=False,
            ),
            "X_ITM": Column(
                dtype=pandera.engines.numpy_engine.Float64,
                checks=[
                    Check.greater_than_or_equal_to(min_value=599999.999),
                    Check.less_than_or_equal_to(max_value=729666.339),
                ],
                nullable=True,
                unique=False,
                coerce=False,
                required=True,
                regex=False,
            ),
            "Y_ITM": Column(
                dtype=pandera.engines.numpy_engine.Float64,
                checks=[
                    Check.greater_than_or_equal_to(min_value=716789.52),
                    Check.less_than_or_equal_to(max_value=4820966.962),
                ],
                nullable=True,
                unique=False,
                coerce=False,
                required=True,
                regex=False,
            ),
        },
        index=Index(
            dtype=pandera.engines.numpy_engine.Int64,
            checks=[
                Check.greater_than_or_equal_to(min_value=0.0),
                Check.less_than_or_equal_to(max_value=53285.0),
            ],
            nullable=False,
            coerce=False,
            name=None,
        ),
        coerce=True,
        strict=False,
        name=None,
    )
    schema(dublin_floor_areas)


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

    weather_dependent_electricity = (
        benchmarks["Typical electricity [kWh/m²y]"]
        * benchmarks["% electricity pro-rated to degree days"]
        * degree_day_factor
    )
    weather_independent_electricity = benchmarks["Typical electricity [kWh/m²y]"] * (
        1 - benchmarks["% electricity pro-rated to degree days"]
    )
    electricity = weather_dependent_electricity + weather_independent_electricity

    # ASSUMPTION: space heat is the only electrical heat
    electricity_heat = (
        weather_dependent_electricity * benchmarks["% suitable for DH or HP"]
    )

    weather_dependent_fossil_fuel = (
        benchmarks["Typical fossil fuel [kWh/m²y]"]
        * benchmarks["% fossil fuel pro-rated to degree days"]
        * degree_day_factor
    )
    weather_independent_fossil_fuel = benchmarks["Typical fossil fuel [kWh/m²y]"] * (
        1 - benchmarks["% fossil fuel pro-rated to degree days"]
    )
    fossil_fuel = weather_dependent_fossil_fuel + weather_independent_fossil_fuel

    # ASSUMPTION: fossil fuel is only used for space heat & hot water
    fossil_fuel_heat = fossil_fuel * benchmarks["% suitable for DH or HP"]

    industrial_low_temperature_heat = (
        benchmarks["Industrial space heat [kWh/m²y]"] * degree_day_factor
        + benchmarks["Industrial process energy [kWh/m²y]"]
        * benchmarks["% suitable for DH or HP"]
    )
    industrial_high_temperature_heat = benchmarks[
        "Industrial process energy [kWh/m²y]"
    ] * (1 - benchmarks["% suitable for DH or HP"])

    normalised_benchmarks = pd.DataFrame(
        {
            "Benchmark": benchmarks["Benchmark"],
            "typical_area_m2": benchmarks["Typical Area [m²]"],
            "area_upper_bound_m2": benchmarks["Area Upper Bound [m²]"],
            "typical_electricity_kwh_per_m2y": electricity,
            "typical_fossil_fuel_kwh_per_m2y": fossil_fuel,
            "typical_building_energy_kwh_per_m2y": benchmarks[
                "Industrial building total [kWh/m²y]"
            ],
            "typical_process_energy_kwh_per_m2y": benchmarks[
                "Industrial process energy [kWh/m²y]"
            ],
            "typical_electricity_heat_kwh_per_m2y": electricity_heat,
            "typical_fossil_fuel_heat_kwh_per_m2y": fossil_fuel_heat,
            "typical_industrial_low_temperature_heat_kwh_per_m2y": industrial_low_temperature_heat,
            "typical_industrial_high_temperature_heat_kwh_per_m2y": industrial_high_temperature_heat,
        }
    )

    normalised_benchmarks.to_csv(product, index=False)


def replace_unexpectedly_large_floor_areas_with_typical_values(
    upstream: Any, product: Any
) -> None:
    buildings = pd.read_csv(upstream["concatenate_local_authority_floor_areas"])
    benchmarks = pd.read_csv(upstream["weather_adjust_benchmarks"])
    with open(upstream["convert_benchmark_uses_to_json"], "r") as f:
        benchmark_uses = json.load(f)

    buildings["Benchmark"] = (
        buildings["Use1"].map(benchmark_uses).rename("Benchmark").fillna("Unknown")
    )
    buildings_with_benchmarks = buildings.merge(benchmarks)

    bounded_area_m2 = buildings_with_benchmarks["Total_SQM"].rename("bounded_area_m2")
    typical_area = buildings_with_benchmarks["typical_area_m2"]

    greater_than_zero_floor_area = buildings_with_benchmarks["Total_SQM"] > 0
    greater_than_typical_benchmark_upper_bound = (
        buildings_with_benchmarks["Total_SQM"]
        > buildings_with_benchmarks["area_upper_bound_m2"]
    )
    valid_benchmark = ~buildings_with_benchmarks["Benchmark"].isin(["Unknown", "None"])
    area_is_greater_than_expected = (
        greater_than_zero_floor_area
        & greater_than_typical_benchmark_upper_bound
        & valid_benchmark
    )
    bounded_area_m2.loc[area_is_greater_than_expected] = typical_area.loc[
        area_is_greater_than_expected
    ]

    propertyno_bounded_area_map = pd.concat(
        [buildings["PropertyNo"], bounded_area_m2], axis=1
    )

    propertyno_bounded_area_map.to_csv(product, index=False)


def save_unknown_benchmark_uses(upstream: Any, product: Any) -> None:
    buildings = pd.read_csv(upstream["concatenate_local_authority_floor_areas"])
    benchmarks = pd.read_csv(upstream["weather_adjust_benchmarks"])
    with open(upstream["convert_benchmark_uses_to_json"], "r") as f:
        benchmark_uses = json.load(f)

    buildings["Benchmark"] = (
        buildings["Use1"].map(benchmark_uses).rename("Benchmark").fillna("Unknown")
    )
    buildings_with_benchmarks = buildings.merge(benchmarks)

    benchmark_is_unknown = buildings_with_benchmarks["Benchmark"] == "Unknown"
    unknown_benchmark_uses = pd.Series(
        buildings_with_benchmarks.loc[benchmark_is_unknown, "Use1"].unique(),
        name="Use1",
    )
    unknown_benchmark_uses.to_csv(product, index=False)


def apply_energy_benchmarks_to_floor_areas(
    upstream: Any, product: Any, boiler_efficiency: float
) -> None:

    buildings = pd.read_csv(upstream["concatenate_local_authority_floor_areas"])
    benchmarks = pd.read_csv(upstream["weather_adjust_benchmarks"])
    with open(upstream["convert_benchmark_uses_to_json"], "r") as f:
        benchmark_uses = json.load(f)

    buildings["Benchmark"] = (
        buildings["Use1"].map(benchmark_uses).rename("Benchmark").fillna("Unknown")
    )
    buildings_with_benchmarks = buildings.merge(benchmarks)

    # Replace invalid floor areas with typical values
    bounded_area_m2 = buildings_with_benchmarks["Total_SQM"].rename("bounded_area_m2")
    greater_than_zero_floor_area = buildings_with_benchmarks["Total_SQM"] > 0
    greater_than_typical_benchmark_upper_bound = (
        buildings_with_benchmarks["Total_SQM"]
        > buildings_with_benchmarks["area_upper_bound_m2"]
    )
    valid_benchmark = ~buildings_with_benchmarks["Benchmark"].isin(["Unknown", "None"])
    area_is_greater_than_expected = (
        greater_than_zero_floor_area
        & greater_than_typical_benchmark_upper_bound
        & valid_benchmark
    )
    bounded_area_m2.loc[area_is_greater_than_expected] = buildings_with_benchmarks[
        "typical_area_m2"
    ].loc[area_is_greater_than_expected]
    buildings_with_benchmarks["bounded_area_m2"] = bounded_area_m2

    # Apply Benchmarks
    kwh_to_mwh = 1e-3
    buildings_with_benchmarks["electricity_demand_mwh_per_y"] = (
        bounded_area_m2.fillna(0)
        * buildings_with_benchmarks["typical_electricity_kwh_per_m2y"].fillna(0)
        * kwh_to_mwh
    ).fillna(0)
    buildings_with_benchmarks["fossil_fuel_demand_mwh_per_y"] = (
        bounded_area_m2.fillna(0)
        * buildings_with_benchmarks["typical_fossil_fuel_kwh_per_m2y"].fillna(0)
        * kwh_to_mwh
        * boiler_efficiency
    ).fillna(0)

    buildings_with_benchmarks["building_energy_mwh_per_y"] = (
        bounded_area_m2.fillna(0)
        * buildings_with_benchmarks["typical_building_energy_kwh_per_m2y"].fillna(0)
        * kwh_to_mwh
    )
    buildings_with_benchmarks["process_energy_mwh_per_y"] = (
        bounded_area_m2.fillna(0)
        * buildings_with_benchmarks["typical_process_energy_kwh_per_m2y"].fillna(0)
        * kwh_to_mwh
    )

    buildings_with_benchmarks["electricity_heat_demand_mwh_per_y"] = (
        bounded_area_m2.fillna(0)
        * buildings_with_benchmarks["typical_electricity_heat_kwh_per_m2y"].fillna(0)
        * kwh_to_mwh
        * boiler_efficiency
    ).fillna(0)
    buildings_with_benchmarks["fossil_fuel_heat_demand_mwh_per_y"] = (
        bounded_area_m2.fillna(0)
        * buildings_with_benchmarks["typical_fossil_fuel_heat_kwh_per_m2y"].fillna(0)
        * kwh_to_mwh
        * boiler_efficiency
    ).fillna(0)
    buildings_with_benchmarks["industrial_low_temperature_heat_demand_mwh_per_y"] = (
        bounded_area_m2.fillna(0)
        * buildings_with_benchmarks[
            "typical_industrial_low_temperature_heat_kwh_per_m2y"
        ].fillna(0)
        * kwh_to_mwh
    )
    buildings_with_benchmarks["industrial_high_temperature_heat_demand_mwh_per_y"] = (
        bounded_area_m2.fillna(0)
        * buildings_with_benchmarks[
            "typical_industrial_high_temperature_heat_kwh_per_m2y"
        ].fillna(0)
        * kwh_to_mwh
    )

    buildings_with_benchmarks.to_csv(product, index=False)


def link_valuation_office_to_small_areas(upstream: Any, product: Any) -> None:
    valuation_office = pd.read_csv(upstream["apply_energy_benchmarks_to_floor_areas"])
    small_area_boundaries = gpd.read_file(
        str(upstream["download_small_area_boundaries"])
    )

    valuation_office_geo = gpd.GeoDataFrame(
        valuation_office,
        geometry=gpd.points_from_xy(
            x=valuation_office["X_ITM"], y=valuation_office["Y_ITM"], crs="EPSG:2157"
        ),
    )
    valuation_office_in_small_areas = gpd.sjoin(
        valuation_office_geo,
        small_area_boundaries[["small_area", "geometry"]],
        op="within",
    ).drop(columns=["geometry", "index_right"])
    valuation_office_in_small_areas.to_csv(product, index=False)


def remove_none_and_unknown_benchmark_buildings(upstream: Any, product: Any) -> None:
    buildings = pd.read_csv(upstream["link_valuation_office_to_small_areas"])
    without_none_or_unknown_benchmarks = buildings.query(
        "Benchmark != ['Unknown', 'None']"
    )
    without_none_or_unknown_benchmarks.to_csv(product)
