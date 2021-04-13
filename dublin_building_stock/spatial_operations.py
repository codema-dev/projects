import geopandas as gpd


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


def convert_to_geodataframe(df, x, y, crs):
    geometry = gpd.points_from_xy(df[x], df[y])
    return gpd.GeoDataFrame(df, geometry=geometry, crs=crs).drop(columns=[x, y])
