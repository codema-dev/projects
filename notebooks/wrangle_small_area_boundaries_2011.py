from pathlib import Path

import geopandas as gpd

from dublin_building_stock.spatial_operations import get_geometries_within


data_dir = Path("../data")


dublin_boundary = gpd.read_file(data_dir / "dublin_boundary.geojson", driver="GeoJSON")
dublin_routing_key_boundaries = gpd.read_file(
    data_dir / "dublin_routing_key_boundaries.geojson",
    driver="GeoJSON",
)


use_columns = ["SMALL_AREA", "EDNAME", "geometry"]
ireland_small_area_boundaries = gpd.read_file(
    data_dir / "Census2011_Small_Areas_generalised20m"
)[use_columns]

dublin_small_area_boundaries = (
    ireland_small_area_boundaries.to_crs(epsg=2157)
    .pipe(get_geometries_within, dublin_boundary.to_crs(epsg=2157))
    .pipe(get_geometries_within, dublin_routing_key_boundaries.to_crs(epsg=2157))
)

dublin_small_area_boundaries.to_file(
    data_dir / "dublin_small_area_boundaries_2011.geojson", driver="GeoJSON"
)
