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

from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import box


def get_geometries_within(left, right):
    left_representative_point = (
        left.geometry.representative_point().rename("geometry").to_frame()
    )
    return (
        gpd.sjoin(left_representative_point, right, op="within")
        .drop(columns=["geometry", "index_right"])
        .merge(left, left_index=True, right_index=True)
        .reset_index(drop=True)
    )


# + tags=["parameters"]
upstream = [
    "download_dublin_small_area_boundaries",
    "unzip_nta_grid_boundaries_data",
]
product = None
# -


nta_grid_boundaries = gpd.read_file(
    str(upstream["unzip_nta_grid_boundaries_data"])
).to_crs(epsg=2157)

dublin_bounding_box = (
    gpd.GeoSeries(box(695000, 712500, 740000, 771000), crs=2157)
    .rename("geometry")
    .to_frame()
)

dublin_nta_grid_boundaries = get_geometries_within(
    nta_grid_boundaries, dublin_bounding_box
)

small_area_boundaries = gpd.read_file(
    str(upstream["download_dublin_small_area_boundaries"])
).to_crs(epsg=2157)

# # Amalgamate NTA grid emissions to Small Areas

# ## Join Centroids within NTA Grid

dublin_nta_grid_points_in_small_areas = get_geometries_within(
    dublin_nta_grid_boundaries,
    small_area_boundaries,
).loc[
    :,
    [
        "NOX",
        "NO2",
        "PM10",
        "PM25",
        "HC",
        "CO",
        "CO2",
        "Benz",
        "Meth",
        "Butad",
        "small_area",
        "geometry",
    ],
]

# ## Convert CO2 to Energy (TFC & TPER)

CO2_TFC = 0.00384527383473325
TFC_TPER = 1.1

dublin_nta_grid_points_in_small_areas["TFC_kWh"] = (
    dublin_nta_grid_points_in_small_areas["CO2"] * CO2_TFC
)
dublin_nta_grid_points_in_small_areas["TPER_kWh"] = (
    dublin_nta_grid_points_in_small_areas["TFC_kWh"] * TFC_TPER
)


total_TFC_GWh = dublin_nta_grid_points_in_small_areas["TFC_kWh"].sum() / 1000000
total_TFC_GWh

# # Estimate All-of-Dublin Road Transport Road Energy


CO2_TFC = 0.00384527383473325
TFC_TPER = 1.1

dublin_nta_grid_boundaries["TFC_kWh"] = dublin_nta_grid_boundaries["CO2"] * CO2_TFC
dublin_nta_grid_boundaries["TPER_kWh"] = (
    dublin_nta_grid_boundaries["TFC_kWh"] * TFC_TPER
)

dublin_nta_grid_boundaries["TPER_kWh"].multiply(10 ** -3).sum()

dublin_nta_grid_boundaries
