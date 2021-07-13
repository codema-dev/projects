from pathlib import Path

from shapely.geometry import Point
import geopandas as gpd

from dublin_building_stock.spatial_operations import get_geometries_within


data_dir = Path("../data")


dublin_boundary = gpd.read_file(data_dir / "dublin_boundary.geojson", driver="GeoJSON")

dublin_routing_key_boundaries = gpd.read_file(
    data_dir / "dublin_routing_key_boundaries.geojson",
    driver="GeoJSON",
)

dublin_local_authority_boundaries = gpd.read_file(
    data_dir / "dublin_local_authority_boundaries.geojson", driver="GeoJSON"
).rename(columns={"COUNTYNAME": "local_authority"})

use_columns = ["SMALL_AREA", "EDNAME", "geometry"]
ireland_small_area_boundaries = gpd.read_file(
    data_dir / "Census2011_Small_Areas_generalised20m"
)[use_columns]

temple_bar_location = Point(715643, 734177)
m_to_km = 1 / 1000
dublin_small_area_boundaries = (
    ireland_small_area_boundaries.to_crs(epsg=2157)
    .pipe(get_geometries_within, dublin_boundary.to_crs(epsg=2157))
    .pipe(
        get_geometries_within,
        dublin_routing_key_boundaries.drop(columns="local_authority").to_crs(epsg=2157),
    )
    .pipe(get_geometries_within, dublin_local_authority_boundaries.to_crs(epsg=2157))
    .assign(
        distance_to_city_centre_in_km=lambda gdf: gdf.geometry.representative_point()
        .distance(temple_bar_location)
        .multiply(m_to_km)
        .round(2)
    )
)

dublin_small_area_boundaries.to_file(
    data_dir / "dublin_small_area_boundaries_2011.geojson", driver="GeoJSON"
)