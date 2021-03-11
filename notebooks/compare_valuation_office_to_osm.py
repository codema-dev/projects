# %%
from urllib.request import urlretrieve
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import osmnx
import pandas as pd

data_dir = Path("../data")


def read_csv_as_gdf(filepath, x, y, crs, *args, **kwargs):
    df = pd.read_csv(filepath, *args, **kwargs)
    return gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df[x], df[y], crs=crs))


# %% [markdown]
# # Get Dublin Boundary
dublin_boundary_filepath = data_dir / "dublin_boundary.geojson"
if not dublin_boundary_filepath.exists():
    urlretrieve(
        url="https://zenodo.org/record/4577018/files/dublin_boundary.geojson",
        filename=dublin_boundary_filepath,
    )

dublin_boundary = gpd.read_file(dublin_boundary_filepath)[["geometry"]].to_crs(
    epsg=2157
)

dublin_boundary_polygon = (
    gpd.read_file(dublin_boundary_filepath).to_crs(epsg=4326).iloc[0].item()
)

# %% [markdown]
# # Get OSM Dublin building footprints
# dublin_building_footprints_filepath = data_dir / "osm_dublin_building_footprint.parquet"
if not dublin_building_footprints_filepath.exists():
    print("Downloading OSM Dublin...")
    dublin_building_footprints = osmnx.geometries_from_polygon(
        dublin_boundary,
        tags={"building": True},
    )
    dublin_building_footprints.drop(columns=["nodes", "ways"]).to_parquet(
        dublin_building_footprints_filepath
    )
else:
    print("Getting OSM Dublin from parquet")
    dublin_building_footprints = gpd.read_parquet(
        dublin_building_footprints_filepath,
    )


# %% [markdown]
# # Get commercial stock 2015
vo_2015 = read_csv_as_gdf(
    data_dir / "commercial_buildings_2015.csv",
    x="X_IG",
    y="Y_IG",
    crs="epsg:2157",
    low_memory=False,
)

# %% [markdown]
# # Get commercial stock within Dublin Boundary
vo_2015_in_dublin_boundary = gpd.sjoin(vo_2015, dublin_boundary, op="within").drop(
    columns="index_right"
)

# %% [markdown]
# # Link commercial stock to OSM building footprints
vo_2015_linked_to_osm = gpd.sjoin(
    dublin_building_footprints.to_crs(epsg=2157),
    vo_2015_in_dublin_boundary,
    op="contains",
)

# %%
vo_2015_linked_to_osm.to_parquet(data_dir / "vo_2015_linked_to_osm_footprints.parquet")

# %%
vo_area = vo_2015_linked_to_osm[["Area (m2)", "geometry"]].assign(
    area_osm=lambda gdf: gdf.geometry.area
)
