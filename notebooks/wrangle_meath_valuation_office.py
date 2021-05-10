from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from dublin_building_stock.non_residential import (
    load_uses_benchmarks,
    load_benchmarks,
)
from dublin_building_stock.spatial_operations import convert_to_geodataframe

DATA_DIR = Path("../data")
KWH_TO_MWH = 10 ** -3

uses_linked_to_benchmarks = load_uses_benchmarks(DATA_DIR)
benchmarks = load_benchmarks(DATA_DIR)

meath_valuation_office_raw = pd.read_excel(DATA_DIR / "meath_valuation_office.ods")
benchmark_columns = [
    "Benchmark",
    "Typical fossil fuel [kWh/m²y]",
    "Typical electricity [kWh/m²y]",
    "Industrial space heat [kWh/m²y]",
    "Industrial building total [kWh/m²y]",
    "Industrial process energy [kWh/m²y]",
    "Typical Area [m²]",
    "Area Upper Bound [m²]",
    "GIA to Sales",
]
industrial_electricity_percentage = 0.38  # SEAI, Energy in Ireland 2020
industrial_fossil_fuel_percentage = 0.62  # SEAI, Energy in Ireland 2020
meath_valuation_office_clean = (
    meath_valuation_office_raw.pipe(
        convert_to_geodataframe, x="X_ITM", y="Y_ITM", crs="EPSG:2157"
    )
    .assign(
        benchmark_1=lambda gdf: gdf["Use1"].map(uses_linked_to_benchmarks).astype(str),
        benchmark_2=lambda gdf: gdf["Use2"].map(uses_linked_to_benchmarks).astype(str),
        ID=lambda df: df["PropertyNo"].astype("int32"),
    )  # link uses to benchmarks so can merge on common benchmarks
    .merge(
        benchmarks[benchmark_columns],
        how="left",
        left_on="benchmark_1",
        right_on="Benchmark",
    )
    .fillna(0)
    .assign(
        bounded_area_m2=lambda df: np.where(
            (df["Total_SQM"] > 5) & (df["Total_SQM"] < df["Area Upper Bound [m²]"]),
            df["Total_SQM"],
            np.nan,
        ),  # Remove all areas outside of 5 <= area <= Upper Bound
        inferred_floor_area_m2=lambda df: np.round(
            df["bounded_area_m2"].fillna(df["Typical Area [m²]"])
            * df["GIA to Sales"].fillna(1)
        ),
        area_is_estimated=lambda df: df["bounded_area_m2"].isnull(),
        industrial_energy_kwh_per_m2_year=lambda df: df[
            "Industrial building total [kWh/m²y]"
        ]
        + df["Industrial process energy [kWh/m²y]"],
        fossil_fuel_mwh_per_year=lambda df: df["Typical fossil fuel [kWh/m²y]"]
        * df["inferred_floor_area_m2"]
        * KWH_TO_MWH
        + df["industrial_energy_kwh_per_m2_year"]
        * industrial_fossil_fuel_percentage
        * df["inferred_floor_area_m2"]
        * KWH_TO_MWH,
        electricity_mwh_per_year=lambda df: df["Typical electricity [kWh/m²y]"]
        * df["inferred_floor_area_m2"]
        * KWH_TO_MWH
        + df["industrial_energy_kwh_per_m2_year"]
        * industrial_electricity_percentage
        * df["inferred_floor_area_m2"]
        * KWH_TO_MWH,
    )
)
