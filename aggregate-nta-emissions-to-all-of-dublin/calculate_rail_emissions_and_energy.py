# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.13.0
#   kernelspec:
#     display_name: Python 3
#     name: python3
# ---

# +
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import box

# -


def sjoin_center_inside(dfA, dfB):
    """Join where center of A intersects B"""
    dfA_center = dfA.copy()
    dfA_center.geometry = dfA.geometry.representative_point()
    dfA_sjoined = gpd.sjoin(dfA_center, dfB, op="intersects")
    return dfA_sjoined.assign(geometry=dfA.geometry)


# + tags=["parameters"]
upstream = [
    "download_dublin_small_area_boundaries",
    "download_dublin_rail_schedules",
    "unzip_nta_rail_links_data",
]
product = None
data_dir = Path("data")
# -

rail_lines = gpd.read_file(str(upstream["unzip_nta_rail_links_data"])).to_crs(epsg=2157)

small_area_boundaries = (
    gpd.read_file(str(upstream["download_dublin_small_area_boundaries"]))
    .loc[:, ["small_area", "geometry"]]
    .to_crs(epsg=2157)
)

dublin_bounding_box = (
    gpd.GeoSeries(box(695000, 712500, 740000, 771000), crs=2157)
    .rename("geometry")
    .to_frame()
)

total_journeys_linkid = pd.read_excel(
    upstream["download_dublin_rail_schedules"]
).fillna(0)

dublin_small_area_boundaries = sjoin_center_inside(
    small_area_boundaries, dublin_bounding_box
)
# -

# # Get Total Journeys

total_journeys = rail_lines.merge(total_journeys_linkid, on="linkID")

total_journeys

# # Distribute Rail lines among Small Areas

# ## Measure line lengths in each Small Area

total_journeys_per_small_area = gpd.overlay(
    total_journeys, small_area_boundaries, "intersection"
)

total_journeys_per_small_area["line_length_km"] = (
    total_journeys_per_small_area.geometry.length * 10 ** -3
)

# +
# ## Link Small Areas to Number of Journeys for linkID
# -

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

diesel_train_kgCO2_per_km = 8.057183256
dart_kgCO2_per_km = 3.793376027
luas_kgCO2_per_km = 1.825367372
diesel_train_kWh_per_km = 30.53119839
dart_kWh_per_km = 11.68991071
luas_kWh_per_km = 5.625169096

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

total_energy = total_journeys_per_small_area["DART_MWh"].sum()
total_energy

total_journeys_per_small_area

# # Estimate All-of-Dublin Rail Energy

total_journeys["line_length_km"] = total_journeys.geometry.length * 10 ** -3

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

total_journeys

for rail_mwh in ["DART_MWh", "LUAS_MWh", "Commuter_MWh", "Intercity_MWh"]:
    print(total_journeys[rail_mwh].sum())

total_journeys_per_small_area.to_file(product["gpkg"], driver="GPKG")
