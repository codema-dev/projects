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

# Adapted from https://geoffboeing.com/2018/04/network-based-spatial-clustering

from ast import literal_eval
from pathlib import Path

import geopandas as gpd
import pandas as pd
from scipy.sparse import csr_matrix
import seaborn as sns
from sklearn.cluster import DBSCAN
from sklearn.metrics import silhouette_score

sns.set()

# + tags=["parameters"]
upstream = [
    "download_dublin_small_area_boundaries",
    "extract_dublin_substations",
    "extract_network_lines",
    "find_nearest_nodes_to_stations_on_network",
    "calculate_path_lengths_along_network_between_substations",
]
product = None
substation_type = "MV"
# -

## Load

small_area_boundaries = gpd.read_file(upstream["download_dublin_small_area_boundaries"])

network = gpd.read_parquet(upstream["extract_network_lines"])

substations = (
    gpd.read_file(str(upstream["extract_dublin_substations"]))
    .query("`Voltage Class` == @substation_type")
    .reset_index(drop=True)
)

nearest_node_ids = (
    pd.read_parquet(upstream["find_nearest_nodes_to_stations_on_network"])
    .squeeze()
    .apply(literal_eval)  # convert "(x,y)" to (x,y) as G uses tuples as keys
)

unique_nearest_node_ids = nearest_node_ids.drop_duplicates()

dirpath = Path(upstream["calculate_path_lengths_along_network_between_substations"])
filenames = list(dirpath.glob("*.parquet"))
# i.e. filenames = ["DIRPATH/0.parquet", DIRPATH/1.parquet", ...]
sorted_filenames = sorted(filenames, key=lambda x: int(x.stem))

node_distance_matrix = pd.concat([pd.read_parquet(f) for f in sorted_filenames], axis=1)

node_distance_matrix.columns = unique_nearest_node_ids
node_distance_matrix.index = unique_nearest_node_ids

# Join with the original nearest_node_ids to retrieve all original substation nodes
# so we have more than just the unique ones!

network_distance_matrix = node_distance_matrix.copy().reindex(
    columns=nearest_node_ids.to_list(), index=nearest_node_ids.to_list()
)

## Cluster

# In a regular distance matrix, zero elements are considered neighbors
# (they're on top of each other). With a sparse matrix only nonzero elements may be
# considered neighbors for DBSCAN. First, make all zeros a very small number instead,
# so we don't ignore them. Otherwise, we wouldn't consider two firms attached to the
# same node as cluster neighbors. Then set everything bigger than epsilon to 0, so we do
# ignore it as we won't consider them neighbors anyway.

# parameterize DBSCAN
eps = 2000  # meters
minpts = 3  # smallest cluster size allowed

network_distance_matrix[network_distance_matrix == 0] = 1
network_distance_matrix[network_distance_matrix > eps] = 0

network_distance_matrix_sparse = csr_matrix(network_distance_matrix)

model = DBSCAN(eps=eps, min_samples=minpts, metric="precomputed")

cluster_ids = model.fit_predict(network_distance_matrix_sparse)

pd.Series(cluster_ids).value_counts()

silhouette_score(network_distance_matrix_sparse, cluster_ids)

use_columns = [
    "Installed Capacity MVA",
    "SLR Load MVA",
    "Demand Available MVA",
    "geometry",
]
clusters = substations[use_columns].join(
    pd.DataFrame({"cluster_ids": cluster_ids, "node_id": nearest_node_ids.apply(str)})
)

## Amalgamate to Zones

small_area_centroids = (
    small_area_boundaries.geometry.representative_point().rename("geometry").to_frame()
)

small_area_substations = small_area_centroids.sjoin_nearest(clusters)

cluster_polygons = (
    pd.concat(
        [small_area_boundaries, small_area_substations.drop(columns="geometry")], axis=1
    )
    .dissolve(by="cluster_ids")
    .reset_index()
)

## Save

clusters.to_file(product["clusters"], driver="GPKG")

## Plot

ax = network.plot(figsize=(40, 40))
clusters.apply(
    lambda x: ax.annotate(
        text=x["cluster_ids"],
        xy=x.geometry.centroid.coords[0],
        ha="center",
        fontsize="large",
        color="red",
    ),
    axis=1,
)

cluster_polygons.plot(column="cluster_ids", figsize=(40, 40))
