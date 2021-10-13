from ast import literal_eval
from os import PathLike
from pathlib import Path
import pickle
from shutil import unpack_archive
from typing import Any
from typing import Dict
from typing import Union

import geopandas as gpd
import momepy
import networkx as nx
import numpy as np
import pandas as pd
from shapely.geometry import Point
from tqdm import tqdm

import geopandas as gpd
import pandas as pd

import geopandas as gpd
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree


def check_file_exists(product: PathLike, filepath: str):
    assert Path(filepath).exists(), f"Please upload {Path(filepath).name} to data/raw"


def unzip_esb_cad_data(product: PathLike, upstream: Dict[str, PathLike]) -> None:
    unpack_archive(
        filename=upstream["check_esb_cad_data_is_uploaded"],
        extract_dir=Path(product).parent,
    )


def convert_mv_lv_data_to_parquet(product: Any, upstream: Dict[str, PathLike]) -> None:
    dublin_mv_index = pd.read_csv(upstream["download_dublin_mv_index"], squeeze=True)
    dirpath = Path(upstream["unzip_esb_cad_data"]) / "Dig Request Style" / "MV-LV Data"
    network = [gpd.read_file(dirpath / f"{id}.dgn") for id in dublin_mv_index]
    mv_lv_network = pd.concat(network)

    # set coordinate reference system to irish grid
    mv_lv_network.crs = "EPSG:29903"

    # convert to irish transverse mercator
    mv_lv_network.to_crs(epsg=2157).to_parquet(product)


def _convert_dataframe_to_geodataframe(
    df: pd.DataFrame,
    x: str,
    y: str,
    from_crs: str,
    to_crs: str = "EPSG:2157",
) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df[x], df[y], crs=from_crs)
    ).to_crs(to_crs)


def extract_dublin_substations(upstream: Any, product: Any) -> None:
    substations = pd.read_csv(upstream["download_esb_substation_capacities"]).pipe(
        _convert_dataframe_to_geodataframe,
        x="Longitude",
        y="Latitude",
        from_crs="EPSG:4326",
        to_crs="EPSG:2157",
    )
    small_area_boundaries = gpd.read_file(
        str(upstream["download_dublin_small_area_boundaries"])
    ).to_crs("EPSG:2157")
    dublin_substations = gpd.sjoin(substations, small_area_boundaries, op="within")
    dublin_substations.to_file(str(product), driver="GPKG")


def extract_dublin_network_lines(upstream: Any, product: Any) -> None:
    network = gpd.read_parquet(upstream["convert_mv_lv_data_to_parquet"]).to_crs(
        epsg=2157
    )
    dublin_boundary = gpd.read_file(str(upstream["download_dublin_boundary"])).to_crs(
        epsg=2157
    )

    mv_network_lines = network.query("Level in [10, 11, 14]")
    dublin_mv_network_lines = mv_network_lines.overlay(
        dublin_boundary, how="intersection"
    )

    # explode converts multi-part geometries to single-part which is req by networkx
    dublin_mv_network_lines.explode(ignore_index=True).to_file(
        str(product), driver="GPKG"
    )


def convert_network_lines_to_networkx(upstream: Any, product: Any) -> None:
    network = gpd.read_file(
        str(upstream["extract_dublin_network_lines"]), driver="GPKG"
    ).dropna(subset=["geometry"])
    G = momepy.gdf_to_nx(network, approach="primal")
    G_dm = nx.DiGraph(G)
    with open(product, "wb") as f:
        pickle.dump(G_dm, f)


def _join_nearest_points(
    gdA: gpd.GeoDataFrame, gdB: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    nA = np.array(list(gdA.geometry.apply(lambda x: (x.x, x.y))))
    nB = np.array(list(gdB.geometry.apply(lambda x: (x.x, x.y))))
    btree = cKDTree(nB)
    _, idx = btree.query(nA, k=1)
    gdB_nearest = gdB.iloc[idx].reset_index(drop=True)
    return pd.concat(
        [
            gdA.reset_index(drop=True).drop(columns="geometry"),
            gdB_nearest,
        ],
        axis=1,
    )


def find_nearest_nodes_to_stations_on_network(
    upstream: Any, product: Any, substation_type: str
) -> None:
    substations = (
        gpd.read_file(str(upstream["extract_dublin_substations"]))
        .query("`Voltage Class` == @substation_type")
        .reset_index(drop=True)
    )
    with open(upstream["convert_network_lines_to_networkx"], "rb") as f:
        G = pickle.load(f)

    nodes_as_points = gpd.GeoDataFrame(
        {"geometry": [Point(n) for n in G.nodes()]}, crs="EPSG:2157"
    )
    nearest_node_points = _join_nearest_points(
        substations[["geometry"]], nodes_as_points
    )
    nearest_node_ids = (
        nearest_node_points.geometry.apply(lambda x: str(x.coords[0]))
        .rename("nearest_node_ids")
        .to_frame()
    )
    nearest_node_ids.to_parquet(product)


def calculate_path_lengths_along_network_between_substations(
    upstream: Any, product: Any
) -> None:
    with open(upstream["convert_network_lines_to_networkx"], "rb") as f:
        G = pickle.load(f)

    nearest_node_ids = (
        pd.read_parquet(upstream["find_nearest_nodes_to_stations_on_network"])
        .squeeze()
        .apply(literal_eval)  # convert "(x,y)" to (x,y) as G uses tuples as keys
    )

    dirpath = Path(product)
    dirpath.mkdir(exist_ok=True)

    unique_nearest_node_ids = nearest_node_ids.drop_duplicates()
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
