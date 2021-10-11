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

# Adapted from https://geoffboeing.com/2018/04/network-based-spatial-clustering/#more-3125

# +
from pathlib import Path

import geopandas as gpd
import momepy
import networkx as nx
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from scipy.sparse import csr_matrix
from shapely.geometry import Point
from tqdm import tqdm

from pathlib import Path

# + tags=["parameters"]
upstream = [
    "extract_dublin_substations",
    "extract_network_lines",
]
product = None
# -

lv_substations = (
    gpd.read_file(str(upstream["extract_dublin_substations"]))
    .query("`Voltage Class` == 'LV'")
    .reset_index(drop=True)
)

network = gpd.read_parquet(upstream["extract_network_lines"])
G = momepy.gdf_to_nx(network, approach="primal")
G = nx.DiGraph(G)

nodes_as_points = gpd.GeoSeries(
    [Point(n) for n in G.nodes()], crs="EPSG:2157", name="geometry"
)

nearest_node_points = lv_substations[["geometry"]].sjoin_nearest(nodes_as_points)

nearest_node_ids = pd.Series(
    pd.concat([nearest_node_points.geometry.x, nearest_node_points.geometry.y], axis=1)
    .to_numpy()
    .tolist(),
    name="nearest_node_ids",
).apply(lambda x: tuple(x))

unique_nearest_node_ids = nearest_node_ids.drop_duplicates()

dirpath = Path(f"../interim/node_to_node_distances")
dirpath.mkdir(exist_ok=True)

for i, origin in enumerate(tqdm(unique_nearest_node_ids)):
    individual_distances = []
    for target in unique_nearest_node_ids:
        try:
            length = nx.dijkstra_path_length(
                G, source=origin, target=target, weight="length"
            )
        except nx.NetworkXNoPath:
            length = np.inf
        individual_distances.append(length)
    all_distances = pd.DataFrame({f"{i}": individual_distances})
    all_distances.to_parquet(dirpath / f"{i}.parquet")

node_distance_matrix = pd.concat(
    [pd.read_parquet(fp) for fp in dirpath.glob("*.parquet")], axis=1
)
node_distance_matrix.columns = unique_nearest_node_ids
node_distance_matrix.index = unique_nearest_node_ids

# Join with the original nearest_node_ids to retrieve all original substation nodes
# so we have more than just the unique ones!

network_distance_matrix = node_distance_matrix.copy().reindex(
    columns=nearest_node_ids.to_list(), index=nearest_node_ids.to_list()
)

# In a regular distance matrix, zero elements are considered neighbors
# (they're on top of each other). With a sparse matrix only nonzero elements may be
# considered neighbors for DBSCAN. First, make all zeros a very small number instead,
# so we don't ignore them. Otherwise, we wouldn't consider two firms attached to the
# same node as cluster neighbors. Then set everything bigger than epsilon to 0, so we do
# ignore it as we won't consider them neighbors anyway.

# parameterize DBSCAN
eps = 2000  # meters
minpts = 3  # smallest cluster size allowed

# +
network_distance_matrix[network_distance_matrix == 0] = 1
network_distance_matrix[network_distance_matrix > eps] = 0

#
# -
network_distance_matrix_sparse = csr_matrix(network_distance_matrix)

model = KMeans(n_clusters=50)
cluster_ids = model.fit_predict(network_distance_matrix_sparse)

pd.Series(cluster_ids).value_counts()

silhouette_score(network_distance_matrix_sparse, cluster_ids)

clusters = lv_substations[["geometry"]].join(
    pd.DataFrame({"cluster_ids": cluster_ids, "node_id": nearest_node_ids.apply(str)})
)

substation_clusters = lv_substations[["geometry"]].join(clusters)

ax = network.plot(figsize=(20, 20))
substation_clusters.query("cluster_ids != 0").apply(
    lambda x: ax.annotate(
        text=x["cluster_ids"],
        xy=x.geometry.centroid.coords[0],
        ha="center",
        size=7.5,
    ),
    axis=1,
)
