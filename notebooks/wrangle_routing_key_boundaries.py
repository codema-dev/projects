from pathlib import Path

import geopandas as gpd

from dublin_building_stock.spatial_operations import get_geometries_within

data_dir = Path("../data")

ireland_routing_key_boundaries = gpd.read_file(
    data_dir / "routingkeys_mi_itm_2016_09_29",
).to_crs(epsg=2157)

dublin_boundary = gpd.read_file(data_dir / "dublin_boundary.geojson", driver="GeoJSON")

dublin_local_authority_boundaries = gpd.read_file(
    data_dir / "dublin_local_authority_boundaries.geojson", driver="GeoJSON"
)

dublin_routing_key_boundaries = (
    get_geometries_within(
        ireland_routing_key_boundaries, dublin_boundary.to_crs(epsg=2157)
    )
    .assign(
        CountyName=lambda gdf: gdf["Descriptor"]
        .str.title()
        .str.replace(r"^(?!Dublin \d+)(.*)$", "Co. Dublin", regex=True)
    )
    .pipe(get_geometries_within, dublin_local_authority_boundaries.to_crs(epsg=2157))
)

dublin_routing_key_boundaries.to_file(
    data_dir / "dublin_routing_key_boundaries.geojson",
    driver="GeoJSON",
)