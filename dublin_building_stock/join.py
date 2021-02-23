import geopandas as gpd


def centroids_within(left, right):

    left_centroids = left.geometry.centroid.rename("geometry").to_frame()
    return (
        gpd.sjoin(left_centroids, right, op="within")
        .drop(columns=["geometry", "index_right"])
        .merge(left, left_index=True, right_index=True)
        .reset_index(drop=True)
    )
