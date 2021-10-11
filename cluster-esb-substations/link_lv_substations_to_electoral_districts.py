# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.13.0
#   kernelspec:
#     display_name: 'Python 3.9.6 64-bit (''lv-grid-capacity'': conda)'
#     name: python3
# ---

import geopandas as gpd
import pandas as pd

def convert_to_geodataframe(
    df: pd.DataFrame,
    x: str,
    y: str,
    from_crs: str,
    to_crs: str = "EPSG:2157",
) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df[x], df[y], crs=from_crs)
    ).to_crs(to_crs)


# + tags=["parameters"]
upstream = [
    "download_esb_substation_capacities",
    "download_dublin_small_area_boundaries",
]
product = None
# -

lv_substation_capacities = (
    pd.read_csv(upstream["download_esb_substation_capacities"])
    .pipe(
        convert_to_geodataframe,
        x="Longitude",
        y="Latitude",
        from_crs="EPSG:4326",
        to_crs="EPSG:2157",
    )
    .query("`Voltage Class` == 'LV'")
)

small_area_boundaries = gpd.read_file(
    upstream["download_dublin_small_area_boundaries"]
).to_crs("EPSG:2157")

electoral_district_boundaries = small_area_boundaries.dissolve(
    by="cso_ed_id", as_index=False
).drop(columns=["small_area"])

lv_substations_in_electoral_districts = gpd.sjoin(
    lv_substation_capacities, electoral_district_boundaries, op="within"
)

use_columns = [
    "SLR Load MVA",
    "Installed Capacity MVA",
    "Demand Available MVA",
]
electoral_district_lv_capacity = (
    lv_substations_in_electoral_districts.groupby("cso_ed_id", as_index=False)[
        use_columns
    ]
    .sum()
    .merge(electoral_district_boundaries)
)


lv_substations_in_electoral_districts.to_csv(product["csv"])

electoral_district_lv_capacity.to_file(product["gpkg"], driver="GPKG")
