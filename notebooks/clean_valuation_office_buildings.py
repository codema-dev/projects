# %%
from collections import defaultdict
from pathlib import Path
from urllib.request import urlretrieve

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from shapely.geometry import box
from valuation_office_ireland.download import download_valuation_office_categories

from dublin_building_stock.download import download
from dublin_building_stock import get_valuation_office_buildings

data_dir = Path("../data")

# %% [markdown]
# # Get 2016 Small Area Boundaries

# %%
dublin_small_area_boundaries_filepath = (
    data_dir / "dublin_small_area_boundaries.geojson"
)
download(
    "https://opendata.arcgis.com/datasets/c85e610da1464178a2cd84a88020c8e2_3.zip",
    dublin_small_area_boundaries_filepath,
)
dublin_small_area_boundaries = gpd.read_file(
    dublin_small_area_boundaries_filepath,
    driver="GeoJSON",
)
# %% [markdown]
# # Get VO Data

# %%
vo_raw = get_valuation_office_buildings(
    data_dir,
    local_authorities=[
        "DUN LAOGHAIRE RATHDOWN CO CO",
        "DUBLIN CITY COUNCIL",
        "FINGAL COUNTY COUNCIL",
        "SOUTH DUBLIN COUNTY COUNCIL",
    ],
)

# %% [markdown]
# # Get Benchmarks

# %% tags=[]
benchmark_uses_dirpath = data_dir / "benchmarks" / "uses"
benchmark_uses = defaultdict()
for filepath in benchmark_uses_dirpath.glob("*.txt"):
    with open(filepath, "r") as file:
        benchmark_uses[filepath.stem] = [line.rstrip() for line in file]

uses_benchmarks = {i: k for k, v in benchmark_uses.items() for i in v}

# %%
benchmarks = pd.read_csv(data_dir / "benchmarks" / "benchmarks.csv")

# %% [markdown]
# # Clean VO Data

# %%
def replace_nan_floor_areas_with_benchmark_median(vo):

    return (
        vo.set_index("benchmark_use_1")["bounded_area"]
        .fillna(vo.groupby("benchmark_use_1")["bounded_area"].median())
        .reset_index(drop=True)
    )


# %%
vo_clean = (
    vo_raw.pipe(
        gpd.GeoDataFrame,
        geometry=gpd.points_from_xy(
            vo_raw[" X ITM"],
            vo_raw[" Y ITM"],
        ),
        crs="epsg:2157",  # initialise Coordinate Reference System as ITM
    )
    .join(vo_raw["Uses"].str.split(", ", expand=True))  # Split 'USE, -' into 'USE', '-'
    .drop(columns=[2, 3])  # both are empty columns
    .rename(
        columns={0: "use_1", 1: "use_2", " Address 1": "name", "Property Number": "id"}
    )
    .assign(
        benchmark_use_1=lambda gdf: gdf["use_1"].map(uses_benchmarks),
        benchmark_use_2=lambda gdf: gdf["use_2"].map(uses_benchmarks),
    )  # link uses to benchmarks so can merge on common benchmarks
    .merge(benchmarks, left_on="benchmark_use_1", right_on="benchmark", how="left")
    .assign(
        # Drop all floor areas < 5m2 or > 35,000m2
        bounded_area=lambda gdf: np.where(
            (gdf["Area"] < 5) | (gdf["Area"] > 35000),
            np.nan,
            gdf["Area"],
        ),
        # Replace empty areas with median or if all empty a typical value
        estimated_area_m2=lambda gdf: gdf.pipe(
            replace_nan_floor_areas_with_benchmark_median
        ).fillna(gdf["typical_floor_area_m2"]),
        estimated_area_km2=lambda gdf: gdf["estimated_area_m2"] * 10 ** -6,
        annual_fossil_fuel_demand_mwh_year=lambda gdf: gdf.eval(
            "typical_fossil_fuel_kwh_year * estimated_area_m2 * 10**-3"
        ),
        annual_electricity_demand_mwh_year=lambda gdf: gdf.eval(
            "typical_electricity_kwh_year * estimated_area_m2 * 10**-3"
        ),
        latitude=lambda gdf: gdf.geometry.to_crs(epsg=4326).y,
        longitude=lambda gdf: gdf.geometry.to_crs(epsg=4326).x,
    )
    .drop_duplicates(
        subset=["id", "name", "use_1", "use_2"]
    )  # if same property listed multiple times
    .pipe(
        gpd.sjoin,
        right_df=dublin_small_area_boundaries[["SMALL_AREA", "geometry"]],
        op="within",
    )  # link each building to its corresponding 2016 Small Area
    .loc[
        :,
        [
            "id",
            "name",
            "Area",
            "estimated_area_m2",
            "estimated_area_km2",
            "annual_fossil_fuel_demand_mwh_year",
            "annual_electricity_demand_mwh_year",
            "use_1",
            "benchmark_use_1",
            "use_2",
            "benchmark_use_2",
            "SEU",
            "Industrial",
            "latitude",
            "longitude",
            "SMALL_AREA",
            "geometry",
        ],
    ]
)

# %% [markdown]
# # Inspect Floor Areas by Benchmark

# %%
max_benchmark_area = (
    vo_clean.groupby("benchmark_use_1")["estimated_area"]
    .max()
    .sort_values(ascending=False)
)
median_benchmark_area = (
    vo_clean.groupby("benchmark_use_1")["estimated_area"]
    .median()
    .sort_values(ascending=False)
)

display(max_benchmark_area)
display(median_benchmark_area)


# %%
def get_benchmark(vo, benchmark_name):
    return (
        vo.groupby("benchmark_use_1")
        .get_group(benchmark_name)
        .sort_values("Area", ascending=False)
    )


# %%
# group = get_benchmark(vo_clean, "Bar Pub or Licensed Club")
# group = get_benchmark(vo_clean, "Entertainment Theatre")
# group = get_benchmark(vo_clean, "Cinema")
group = get_benchmark(vo_clean, "Covered Car Park")
# group = get_benchmark(vo_clean, "Hotels Small")
# group = get_benchmark(vo_clean, "Public Buildings Museums & Art Galleries")
# group = get_benchmark(vo_clean, "Retail Department Store")
# group = get_benchmark(vo_clean, "Storage Facility")

display(group)

# %% [markdown]
# # Save

# %%
vo_clean.to_file(
    data_dir / "dublin_valuation_office_buildings.geojson", driver="GeoJSON"
)
vo_clean.to_csv(data_dir / "dublin_valuation_office_buildings.csv")
