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
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

# + tags=["parameters"]
upstream = ["extract_dublin_substations", "download_dublin_small_area_boundaries"]
product = None
n_clusters = None
# -

## Load

lv_substations = (
    gpd.read_file(str(upstream["extract_dublin_substations"]))
    .query("`Voltage Class` == 'LV'")
    .reset_index(drop=True)
)

small_area_boundaries = gpd.read_file(
    str(upstream["download_dublin_small_area_boundaries"]), driver="GPKG"
)

## Cluster

points = pd.DataFrame(
    {"x": lv_substations.geometry.x, "y": lv_substations.geometry.y}
).to_numpy()

model = KMeans(n_clusters=n_clusters)
cluster_ids = model.fit_predict(points)

silhouette_score(points, cluster_ids)

pd.Series(cluster_ids).value_counts().tail()

use_columns = small_area_boundaries.columns.to_list() + [
    "Installed Capacity MVA",
    "SLR Load MVA",
    "Demand Available MVA",
]
substation_clusters = lv_substations[use_columns].join(
    pd.DataFrame({"cluster_ids": cluster_ids})
)

cluster_sizes = (
    substation_clusters.groupby("cluster_ids").size().rename("number_of_substations")
)

cluster_demands = substation_clusters.groupby("cluster_ids").sum()

cluster_statistics = pd.concat([cluster_sizes, cluster_demands], axis=1)

substation_clusters.plot(c=substation_clusters["cluster_ids"], figsize=(20, 20))

small_area_centroids = (
    small_area_boundaries.geometry.representative_point().rename("geometry").to_frame()
)

small_area_substations = small_area_centroids.sjoin_nearest(
    substation_clusters[["cluster_ids", "geometry"]]
)

cluster_polygons = (
    pd.concat([small_area_boundaries, small_area_substations["cluster_ids"]], axis=1)
    .dissolve(by="cluster_ids")
    .join(cluster_statistics)
    .reset_index()
)

cluster_polygons.plot(column="cluster_ids", figsize=(20, 20))

## Save

cluster_statistics.to_csv(product["summary"], index=False)

substation_clusters.to_file(str(product["raw"]), driver="GPKG")

cluster_polygons.to_file(str(product["gpkg"]), driver="GPKG")

## Plot silhouette score for different numbers of clusters

n_clusters = [2, 10, 20, 40, 100]
scores = []
for i in n_clusters:
    model = KMeans(n_clusters=i)
    cluster_ids = model.fit_predict(points)
    scores.append(silhouette_score(points, cluster_ids))
pd.Series(scores, index=n_clusters).plot()
