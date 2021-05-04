# %%
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from dublin_building_stock.spatial_operations import convert_to_geodataframe
from dublin_building_stock.spatial_operations import get_geometries_within

data_dir = Path("../data")
kwh_to_mwh_conversion_factor = 10 ** -3

# %%
dublin_routing_key_boundaries = gpd.read_file(
    data_dir / "dublin_routing_key_boundaries.geojson", driver="GeoJSON"
)

# %%
dublin_small_area_boundaries_2011 = gpd.read_file(
    data_dir / "Dublin_Census2011_Small_Areas_generalised20m"
).pipe(get_geometries_within, dublin_routing_key_boundaries)

# %%
industrial_electricity_percentage = 0.38  # SEAI, Energy in Ireland 2020
industrial_fossil_fuel_percentage = 0.62  # SEAI, Energy in Ireland 2020
use_columns = [
    "ID",
    "Property Use",
    "Benchmark",
    "Industrial",
    "inferred_floor_area_m2",
    "estimated_fossil_fuel_mwh_per_year",
    "estimated_electricity_mwh_per_year",
    "latitude",
    "longitude",
    "COUNTYNAME",
]
vo_private = (
    pd.read_csv(data_dir / "valuation_office_private.csv")
    .rename(
        columns={
            "inferred_area_m2": "inferred_floor_area_m2",
        }
    )
    .assign(
        ID=lambda df: df["ID"].astype("float64"),
        industrial_total_kwh_per_m2_year=lambda df: df[
            "Industrial building total [kWh/m²y]"
        ]
        + df["Industrial process energy [kWh/m²y]"],
        estimated_fossil_fuel_mwh_per_year=lambda df: df[
            "Typical fossil fuel [kWh/m²y]"
        ].fillna(0)
        * df["inferred_floor_area_m2"]
        * kwh_to_mwh_conversion_factor
        + df["industrial_total_kwh_per_m2_year"].fillna(0)
        * industrial_fossil_fuel_percentage
        * df["inferred_floor_area_m2"]
        * kwh_to_mwh_conversion_factor,
        estimated_electricity_mwh_per_year=lambda df: df[
            "Typical electricity [kWh/m²y]"
        ].fillna(0)
        * df["inferred_floor_area_m2"]
        * kwh_to_mwh_conversion_factor
        + df["industrial_total_kwh_per_m2_year"].fillna(0)
        * industrial_electricity_percentage
        * df["inferred_floor_area_m2"]
        * kwh_to_mwh_conversion_factor,
    )
    .pipe(convert_to_geodataframe, x="longitude", y="latitude", crs="EPSG:4326")
    .to_crs(epsg=2157)
    .pipe(gpd.sjoin, dublin_routing_key_boundaries, op="within")
    .drop(columns="index_right")
    .to_crs(epsg=4326)
    .assign(latitude=lambda gdf: gdf.geometry.y, longitude=lambda gdf: gdf.geometry.x)
    .loc[:, use_columns]
)


# %%
use_columns = [
    "building_type",
    "COUNTYNAME",
    "Benchmark",
    "address",
    "uncertain_lat_long",
    "metered_fossil_fuel_mwh_per_year",
    "metered_electricity_mwh_per_year",
    "latitude",
    "longitude",
]
m_and_r = (
    pd.read_csv(data_dir / f"FOI_Codema_24.1.20_nominatim.csv")
    .assign(
        uncertain_lat_long=lambda df: df["nominatim_address"].isnull(),
        Benchmark=lambda df: df["category"].replace(
            {
                "Education Building": "Schools and seasonal public buildings",
                "Office Building": "General office",
                "Other Building": "Other (M&R)",
                "Healthcare Building": "Hospital (clinical and research)",
            }
        ),
        building_type="public_sector",
    )
    .rename(columns={"postcode": "COUNTYNAME"})
    .assign(
        metered_fossil_fuel_mwh_per_year=lambda df: df["gas_kwh_2018"]
        .abs()
        .multiply(kwh_to_mwh_conversion_factor)
        .fillna(0),
        metered_electricity_mwh_per_year=lambda df: df["electricity_kwh_2018"]
        .abs()
        .multiply(kwh_to_mwh_conversion_factor)
        .fillna(0),
    )  # remove negative demands
    .loc[:, use_columns]
)

# %%
use_columns = [
    "ID",
    "address",
    "metered_fossil_fuel_mwh_per_year",
    "metered_electricity_mwh_per_year",
]
fossil_fuel_columns = [
    "Diesel Use [kWh/y]",
    "Gas Oil [kWh/y]",
    "Light Fuel Oil Use [kWh/y]",
    "Heavy Fuel Oil Use [kWh/y]",
    "Natural Gas Use [kWh/y]",
]
epa_industrial_sites = (
    pd.read_excel(data_dir / "epa_industrial_sites.xlsx")
    .pipe(convert_to_geodataframe, y="Latitude", x="Longitude", crs="EPSG:4326")
    .assign(
        metered_fossil_fuel_mwh_per_year=lambda gdf: gdf[fossil_fuel_columns]
        .sum(axis=1)
        .multiply(kwh_to_mwh_conversion_factor),
        metered_electricity_mwh_per_year=lambda gdf: gdf["Electricity Use [kWh/y]"]
        * kwh_to_mwh_conversion_factor,
    )
    .rename(columns={"Address": "address", "Valuation Office ID": "ID"})
    .loc[:, use_columns]
)

# %%
# Eirgrid All Island Generation capacity statement 2020 - 2029
twh_to_mwh_conversion_factor = 10 ** 6
median_all_ireland_data_centre_demand_2021 = 32.5 * twh_to_mwh_conversion_factor
data_centres = pd.DataFrame(
    {
        "building_type": ["data_centre"],
        "metered_electricity_mwh_per_year": [all_ireland_data_centre_demand_2021],
    }
)

# %%
non_residential_stock = (
    pd.concat(
        [
            vo_private.merge(epa_industrial_sites, how="left"),
            m_and_r,
            data_centres,
        ]
    )
    .assign(
        inferred_fossil_fuel_mwh_per_year=lambda df: df[
            "metered_fossil_fuel_mwh_per_year"
        ]
        .fillna(df["estimated_fossil_fuel_mwh_per_year"])
        .fillna(0),
        inferred_electricity_mwh_per_year=lambda df: df[
            "metered_electricity_mwh_per_year"
        ]
        .fillna(df["estimated_electricity_mwh_per_year"])
        .fillna(0),
        inferred_energy_mwh_per_year=lambda df: df["inferred_fossil_fuel_mwh_per_year"]
        + df["inferred_electricity_mwh_per_year"],
    )
    .reset_index(drop=True)
)

# %%
industrial_stock = non_residential_stock.query(
    "Industrial == 1 and Benchmark != 'Data Centre'"
).assign(building_type="industrial")

# %%
industrial_stock.to_csv(data_dir / "industrial_stock.csv", index=False)

# %%
data_centres = non_residential_stock.query("building_type == 'data_centre'")

# %%
public_sector_stock = non_residential_stock.query("building_type == 'public_sector'")

# %%
public_sector_stock.to_csv(data_dir / "public_sector_stock.csv", index=False)

# %%
commercial_stock = non_residential_stock.query(
    "Industrial != 1 and building_type != 'public_sector'"
).assign(building_type="commercial")


# %%
commercial_stock.to_csv(data_dir / "commercial_stock.csv", index=False)

# %%
kwh_to_mwh_conversion_factor = 10 ** -3
typical_boiler_efficiency = 0.85
use_columns = [
    "building_type",
    "dwelling_type",
    "period_built",
    "inferred_floor_area_m2",
    "inferred_ber",
    "energy_kwh_per_m2_year",
    "inferred_fossil_fuel_mwh_per_year",
    "inferred_electricity_mwh_per_year",
    "inferred_energy_mwh_per_year",
    "SMALL_AREA_2011",
    "COUNTYNAME",
]
residential_stock = (
    pd.read_csv(data_dir / "dublin_indiv_hh.csv", low_memory=False)
    .rename(
        columns={
            "inferred_floor_area": "inferred_floor_area_m2",
            "heating_mwh_per_year": "estimated_heating_mwh_per_year",
        }
    )
    .merge(
        dublin_small_area_boundaries_2011[["SMALL_AREA", "COUNTYNAME"]], on="SMALL_AREA"
    )
    .drop(columns="SMALL_AREA_2011")
    .rename(
        columns={
            "SMALL_AREA": "SMALL_AREA_2011",
        }
    )
    .assign(
        estimated_fossil_fuel_mwh_per_year=lambda df: df["energy_kwh_per_m2_year"]
        * df["inferred_floor_area_m2"]
        * typical_boiler_efficiency
        * kwh_to_mwh_conversion_factor,
        estimated_electricity_mwh_per_year=5,
        inferred_electricity_mwh_per_year=lambda df: df[
            "estimated_electricity_mwh_per_year"
        ]
        .fillna(0)
        .astype("float64"),
        inferred_fossil_fuel_mwh_per_year=lambda df: df[
            "estimated_fossil_fuel_mwh_per_year"
        ]
        .fillna(0)
        .astype("float64"),
        inferred_energy_mwh_per_year=lambda df: df["inferred_fossil_fuel_mwh_per_year"]
        + df["inferred_electricity_mwh_per_year"],
        building_type="residential",
    )
    .loc[:, use_columns]
    .reset_index(drop=True)
)

# %%
residential_stock.to_csv(data_dir / "residential_stock.csv", index=False)

# %%
all_stock = pd.concat(
    [
        residential_stock,
        commercial_stock,
        data_centres,
        industrial_stock,
        public_sector_stock,
    ]
)

# %%
all_stock.to_csv(data_dir / "all_stock.csv", index=False)

# %%
use_columns = [
    "building_type",
    "Property Use",
    "Benchmark",
    "address",
    "inferred_floor_area_m2",
    "estimated_fossil_fuel_mwh_per_year",
    "metered_fossil_fuel_mwh_per_year",
    "estimated_electricity_mwh_per_year",
    "metered_electricity_mwh_per_year",
    "inferred_energy_mwh_per_year",
]
seus = (
    all_stock[use_columns]
    .sort_values("inferred_energy_mwh_per_year", ascending=False)
    .iloc[:200]
)

# %%
seus.to_csv(data_dir / "top_200_seus.csv", index=False)

# %%
all_stock_without_power_plants = all_stock.query(
    "`Property Use` != 'GENERATING STATION'"
)

# %% [markdown]
# # Plot sectoral breakdown

# %%
overview_electricity = all_stock_without_power_plants.groupby("building_type")[
    "inferred_electricity_mwh_per_year"
].sum()

# %%
overview_electricity.loc["rail"] = 46295 + 44668  # DART + LUAS

# %%
overview_fossil_fuel = all_stock.groupby("building_type")[
    "inferred_fossil_fuel_mwh_per_year"
].sum()

# %%
# Converting emissions from NTA Model to demand
# ... assuming CO2_TFC = 0.00384527383473325, TFC_TPER = 1.1
overview_fossil_fuel.loc["road_transport"] = 6444154.24

# %%
overview_fossil_fuel.loc["rail"] = 69047 + 15787  # Commuter + Intercity

# %%
overview = pd.concat([overview_electricity, overview_fossil_fuel], axis=1)

# %%
import matplotlib.patheffects as pe
import matplotlib.pyplot as plt
import seaborn as sns

sns.set()

# %%
f, ax = plt.subplots(figsize=(10, 10))
total = overview_electricity.sum()
percentages = [str(round((x / total) * 100, 1)) + "%" for x in overview_electricity]
overview_electricity.plot.pie(ax=ax, labels=percentages, fontsize=15, legend=True)

# %%
f, ax = plt.subplots(figsize=(30, 30))
overview_electricity.plot.pie(ax=ax, fontsize=20)

# %%
f, ax = plt.subplots(figsize=(30, 30))
overview_fossil_fuel.plot.pie(ax=ax, fontsize=20)

# %%
overview.to_csv(data_dir / "overview.csv")

# %%
