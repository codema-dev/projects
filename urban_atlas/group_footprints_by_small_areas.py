# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.5
#   kernelspec:
#     display_name: 'Python 3.9.6 64-bit (''urban-atlas'': conda)'
#     name: python3
# ---

# %%
import geopandas as gpd

# %% tags=["parameters"]
upstream = {
    "download_small_areas": "data/external/dublin_small_area_boundaries_in_routing_keys.gpkg",
    "upload_urban_atlas": "data/raw/Urban Atlas",
}
product = {"data": "data/processed/urban_atlas_small_area_item_area.csv.gz"}

# %%
urban_atlas = gpd.read_file(upstream["upload_urban_atlas"])

# %%
small_areas = gpd.read_file(upstream["download_small_areas"])

# %%
urban_atlas_representative_points = (
    urban_atlas.geometry.representative_point()
    .to_frame()
    .rename(columns={0: "geometry"})
)

# %%
urban_atlas_in_small_areas = (
    gpd.sjoin(
        urban_atlas_representative_points.to_crs(epsg=2157),
        small_areas.to_crs(epsg=2157),
        op="within",
    )
    .drop(columns="geometry")
    .join(urban_atlas)
    .drop(columns="index_right")
)

# %%
urban_atlas_small_area_item_area = (
    urban_atlas_in_small_areas.groupby(["small_area", "ITEM"])
    .agg({"geometry": lambda x: x.area.sum()})
    .rename(columns={"geometry": "area_m2"})
)

# %%
urban_atlas_small_area_item_area.to_csv(
    product["data"]["urban_atlas_small_area_item_area"]
)
