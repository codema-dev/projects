from pathlib import Path

import geopandas as gpd

from dublin_building_stock.spatial_operations import get_geometries_within

data_dir = Path("../data")

ireland_routing_key_boundaries = gpd.read_file(
    data_dir / "routingkeys_mi_itm_2016_09_29",
).to_crs(epsg=2157)


ireland_local_authority_boundaries = gpd.read_file(
    data_dir / "Census2011_Admin_Counties_generalised20m"
)
dublin_local_authorities = [
    "Dublin City",
    "South Dublin",
    "Fingal",
    "Dún Laoghaire-Rathdown",
]
dublin_local_authority_boundaries = ireland_local_authority_boundaries.query(
    f"COUNTYNAME in {dublin_municipalities}"
)[["COUNTYNAME", "geometry"]]

dublin_local_authority_boundaries.to_file(
    data_dir / "dublin_local_authority_boundaries.geojson", driver="GeoJSON"
)