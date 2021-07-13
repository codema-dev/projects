from pathlib import Path

import geopandas as gpd
from shapely.geometry import Point

from dublin_building_stock.spatial_operations import get_geometries_within

data_dir = Path("../data")

dublin_routing_key_boundaries = gpd.read_file(
    data_dir / "dublin_routing_key_boundaries.geojson",
    driver="GeoJSON",
)

temple_bar_location = Point(715643, 734177)
m_to_km = 1 / 1000
dublin_postcode_boundaries = (
    dublin_routing_key_boundaries.dissolve(by="CountyName", as_index=False)
    .drop(columns=["RoutingKey", "Descriptor"])
    .assign(
        distance_to_city_centre_in_km=lambda gdf: gdf.geometry.representative_point()
        .distance(temple_bar_location)
        .multiply(m_to_km)
        .round(2)
    )
)

dublin_postcode_boundaries.to_file(
    data_dir / "dublin_postcode_boundaries.geojson", driver="GeoJSON"
)
