# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.11.4
#   kernelspec:
#     display_name: Python 3
#     name: python3
# ---

# +
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import box

# +
data_dir = Path("data")
FILEPATHS = {
    "rail_links": data_dir / "external" / "Dublin_Rail_Links" / "Dublin_Rail_Links.shp",
    "small_area_boundaries": data_dir
    / "external"
    / "dublin_small_area_boundaries_in_routing_keys.gpkg",
    "rail_schedules": data_dir
    / "external"
    / "Transport Emissions - Combined Rail Schedules.xlsx",
}

# + [markdown]
# # Read input data
#
# - NTA Rail Line Statistics
# - 2016 Small Area Boundaries
# - Dublin Boundary
#
# Notes:
# - Convert spatial data to ITM or epsg=2157 so both in same Coordinate Reference System
# - **Don't forget to Mount Google Drive as otherwise this Notebook won't be able to access the data**

# +
rail_lines = gpd.read_file(FILEPATHS["rail_links"]).to_crs(epsg=2157)

# + id="BMPv0-FLWtVu"
small_area_boundaries = gpd.read_file(FILEPATHS["small_area_boundaries"])[
    ["small_area", "geometry"]
].to_crs(epsg=2157)

# +
dublin_bounding_box = (
    gpd.GeoSeries(box(695000, 712500, 740000, 771000), crs=2157)
    .rename("geometry")
    .to_frame()
)

# +
total_journeys_linkid = pd.read_excel(FILEPATHS["rail_schedules"])

# +
def sjoin_center_inside(dfA, dfB):
    """Join where center of A intersects B"""
    dfA_center = dfA.copy()
    dfA_center.geometry = dfA.geometry.representative_point()
    dfA_sjoined = gpd.sjoin(dfA_center, dfB, op="intersects")
    return dfA_sjoined.assign(geometry=dfA.geometry)


dublin_small_area_boundaries = sjoin_center_inside(
    small_area_boundaries, dublin_bounding_box
)

# + [markdown]
# # Get Total Journeys

# +
total_journeys = rail_lines.merge(total_journeys_linkid, on="linkID")

# +
total_journeys

# + [markdown]
# # Distribute Rail lines among Small Areas

# + [markdown]
# ## Measure line lengths in each Small Area

# +
total_journeys_per_small_area = gpd.overlay(
    total_journeys, small_area_boundaries, "union"
)

# +
total_journeys_per_small_area["line_length_km"] = (
    total_journeys_per_small_area.geometry.length * 10 ** -3
)

# +
# ## Link Small Areas to Number of Journeys for linkID

# +
total_journeys_per_small_area["DART_total"] = (
    total_journeys_per_small_area["DART_northbound"]
    + total_journeys_per_small_area["DART_southbound"]
)
total_journeys_per_small_area["LUAS_total"] = (
    total_journeys_per_small_area["LUAS_northbound"]
    + total_journeys_per_small_area["LUAS_southbound"]
)
total_journeys_per_small_area["Commuter_total"] = (
    total_journeys_per_small_area["Commuter_northbound"]
    + total_journeys_per_small_area["Commuter_southbound"]
)
total_journeys_per_small_area["Intercity_total"] = (
    total_journeys_per_small_area["Intercity_northbound"]
    + total_journeys_per_small_area["Intercity_southbound"]
)

# +
diesel_train_kgCO2_per_km = 8.057183256
dart_kgCO2_per_km = 3.793376027
luas_kgCO2_per_km = 1.825367372
diesel_train_kWh_per_km = 30.53119839
dart_kWh_per_km = 11.68991071
luas_kWh_per_km = 5.625169096

# +
total_journeys_per_small_area["DART_MWh"] = (
    total_journeys_per_small_area["DART_total"]
    * total_journeys_per_small_area["line_length_km"]
    * dart_kWh_per_km
    * 10 ** -3
)
total_journeys_per_small_area["DART_tCO2"] = (
    total_journeys_per_small_area["DART_total"]
    * total_journeys_per_small_area["line_length_km"]
    * dart_kgCO2_per_km
    * 10 ** -3
)
total_journeys_per_small_area["LUAS_MWh"] = (
    total_journeys_per_small_area["LUAS_total"]
    * total_journeys_per_small_area["line_length_km"]
    * luas_kWh_per_km
    * 10 ** -3
)
total_journeys_per_small_area["LUAS_tCO2"] = (
    total_journeys_per_small_area["LUAS_total"]
    * total_journeys_per_small_area["line_length_km"]
    * luas_kgCO2_per_km
    * 10 ** -3
)
total_journeys_per_small_area["Commuter_MWh"] = (
    total_journeys_per_small_area["Commuter_total"]
    * total_journeys_per_small_area["line_length_km"]
    * diesel_train_kWh_per_km
    * 10 ** -3
)
total_journeys_per_small_area["Commuter_tCO2"] = (
    total_journeys_per_small_area["Commuter_total"]
    * total_journeys_per_small_area["line_length_km"]
    * diesel_train_kgCO2_per_km
    * 10 ** -3
)
total_journeys_per_small_area["Intercity_MWh"] = (
    total_journeys_per_small_area["Intercity_total"]
    * total_journeys_per_small_area["line_length_km"]
    * diesel_train_kWh_per_km
    * 10 ** -3
)
total_journeys_per_small_area["Intercity_tCO2"] = (
    total_journeys_per_small_area["Intercity_total"]
    * total_journeys_per_small_area["line_length_km"]
    * diesel_train_kgCO2_per_km
    * 10 ** -3
)

# +
total_energy = total_journeys_per_small_area["DART_MWh"].sum()
total_energy

# +
total_journeys_per_small_area

# +
total_journeys_per_small_area.to_file(
    data_dir / "rail_small_area_statistics.geojson", driver="GeoJSON"
)

# + [markdown]
# # Estimate All-of-Dublin Rail Energy

# +
total_journeys["line_length_km"] = total_journeys.geometry.length * 10 ** -3

# +
total_journeys["DART_total"] = (
    total_journeys["DART_northbound"] + total_journeys["DART_southbound"]
)
total_journeys["LUAS_total"] = (
    total_journeys["LUAS_northbound"] + total_journeys["LUAS_southbound"]
)
total_journeys["Commuter_total"] = (
    total_journeys["Commuter_northbound"] + total_journeys["Commuter_southbound"]
)
total_journeys["Intercity_total"] = (
    total_journeys["Intercity_northbound"] + total_journeys["Intercity_southbound"]
)

# +
total_journeys["DART_MWh"] = (
    total_journeys["DART_total"]
    * total_journeys["line_length_km"]
    * dart_kWh_per_km
    * 10 ** -3
)
total_journeys["DART_tCO2"] = (
    total_journeys["DART_total"]
    * total_journeys["line_length_km"]
    * dart_kgCO2_per_km
    * 10 ** -3
)
total_journeys["LUAS_MWh"] = (
    total_journeys["LUAS_total"]
    * total_journeys["line_length_km"]
    * luas_kWh_per_km
    * 10 ** -3
)
total_journeys["LUAS_tCO2"] = (
    total_journeys["LUAS_total"]
    * total_journeys["line_length_km"]
    * luas_kgCO2_per_km
    * 10 ** -3
)
total_journeys["Commuter_MWh"] = (
    total_journeys["Commuter_total"]
    * total_journeys["line_length_km"]
    * diesel_train_kWh_per_km
    * 10 ** -3
)
total_journeys["Commuter_tCO2"] = (
    total_journeys["Commuter_total"]
    * total_journeys["line_length_km"]
    * diesel_train_kgCO2_per_km
    * 10 ** -3
)
total_journeys["Intercity_MWh"] = (
    total_journeys["Intercity_total"]
    * total_journeys["line_length_km"]
    * diesel_train_kWh_per_km
    * 10 ** -3
)
total_journeys["Intercity_tCO2"] = (
    total_journeys["Intercity_total"]
    * total_journeys["line_length_km"]
    * diesel_train_kgCO2_per_km
    * 10 ** -3
)

# +
total_journeys

# +
for rail_mwh in ["DART_MWh", "LUAS_MWh", "Commuter_MWh", "Intercity_MWh"]:
    print(total_journeys[rail_mwh].sum())
