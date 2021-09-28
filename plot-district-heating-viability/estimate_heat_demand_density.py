from pathlib import Path

import geopandas as gpd
import pandas as pd

# + tags=["parameters"]
upstream = [
    "download_synthetic_bers",
    "download_valuation_office_energy_estimates",
    "download_dublin_small_area_boundaries",
]
product = None
# -

Path(product["density"]).parent.mkdir(exist_ok=True)

## Load

residential = pd.read_parquet(upstream["download_synthetic_bers"])

commercial_and_industrial = pd.read_csv(
    upstream["download_valuation_office_energy_estimates"]
)

small_area_boundaries = gpd.read_file(
    str(upstream["download_dublin_small_area_boundaries"]), driver="GPKG"
).set_index("small_area")

## Globals

kwh_to_mwh = 1e-3
m2_to_km2 = 1e-6
mwh_to_tj = 3.6e-3

## Small Area Statistics

### Buildings

residential_small_area_count = residential.groupby("small_area").size()

is_industrial = commercial_and_industrial["process_energy_mwh_per_y"] > 0
industrial_small_area_count = (
    commercial_and_industrial[is_industrial].groupby("small_area").size()
)
commercial_small_area_count = (
    commercial_and_industrial[~is_industrial].groupby("small_area").size()
)

building_count = pd.concat(
    [
        residential_small_area_count.rename("residential_buildings"),
        commercial_small_area_count.rename("commercial_buildings"),
        industrial_small_area_count.rename("industrial_buildings"),
    ],
    axis=1,
).fillna(0)

building_count

### Floor Areas

columns = [c for c in residential.columns if "area" in c]
residential_small_area_floor_area = (
    residential.groupby("small_area")[columns].sum().sum(axis=1)
)

industrial_small_area_floor_area = (
    commercial_and_industrial[is_industrial]
    .groupby("small_area")["bounded_area_m2"]
    .sum()
)

commercial_small_area_floor_area = (
    commercial_and_industrial[~is_industrial]
    .groupby("small_area")["bounded_area_m2"]
    .sum()
)

building_floor_areas = pd.concat(
    [
        residential_small_area_floor_area.rename("residential_floor_area_m2"),
        industrial_small_area_floor_area.rename("commercial_floor_area_m2"),
        commercial_small_area_floor_area.rename("industrial_floor_area_m2"),
    ],
    axis=1,
).fillna(0)

building_floor_areas

## Calculate Heat Demand Density

small_area_boundary_area_km2 = small_area_boundaries.geometry.area * m2_to_km2

columns = [
    c for c in residential.columns if any(x in c for x in ["sh_demand", "hw_demand"])
]
residential_heat_mwh = (
    residential.groupby("small_area")[columns].sum().sum(axis=1).multiply(kwh_to_mwh)
)

# ASSUMPTION: Average commercial boiler efficiency is the same as average residential
# ... Building Energy Ratings dataset, SEAI 2021
# assumed_boiler_efficiency is 0.85
columns = [
    "fossil_fuel_heat_demand_mwh_per_y",
    "electricity_heat_demand_mwh_per_y",
]
commercial_heat_mwh = (
    commercial_and_industrial.groupby("small_area")[columns].sum().sum(axis=1)
)

industrial_heat_mwh = commercial_and_industrial.groupby("small_area")[
    "industrial_low_temperature_heat_demand_mwh_per_y"
].sum()

heat_mwh = pd.concat(
    [
        residential_heat_mwh.rename("residential_heat_mwh"),
        commercial_heat_mwh.rename("commercial_heat_mwh"),
        industrial_heat_mwh.rename("industrial_heat_mwh"),
    ],
    axis=1,
).fillna(0)

heat_tj_per_km2 = (
    heat_mwh.divide(small_area_boundary_area_km2, axis=0)
    .multiply(mwh_to_tj)
    .rename(columns=lambda c: c.replace("mwh", "tj_per_km2"))
)

## Save

demand_stats = pd.concat(
    [building_count, heat_mwh],
    axis=1,
).dropna()

demand_stats.to_csv(product["demand"])

demand_stats

density_stats = pd.concat(
    [
        building_count,
        building_floor_areas,
        small_area_boundary_area_km2.rename("boundary_area_km2"),
        heat_tj_per_km2,
    ],
    axis=1,
).dropna()

density_stats.to_csv(product["density"])

density_stats
