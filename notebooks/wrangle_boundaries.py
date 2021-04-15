# %%
from pathlib import Path

import geopandas as gpd

from dublin_building_stock.boundaries import create_dublin_small_area_boundaries
from dublin_building_stock.spatial_operations import get_geometries_within

data_dir = Path("../data")

# %%
dublin_boundary = gpd.read_file(data_dir / "dublin_boundary.geojson", driver="GeoJSON")
create_dublin_small_area_boundaries(data_dir, dublin_boundary)

# %%
dublin_small_area_boundaries_2011 = gpd.read_file(
    data_dir / "Dublin_Census2011_Small_Areas_generalised20m"
)
dublin_small_area_boundaries_2016 = gpd.read_file(
    data_dir
    / "Dublin_Small_Areas_Ungeneralised_-_OSi_National_Statistical_Boundaries_-_2015-shp"
)
small_areas_2011_vs_2011 = (
    get_geometries_within(
        dublin_small_area_boundaries_2016,
        dublin_small_area_boundaries_2011[["SMALL_AREA", "geometry"]],
    )
    .rename(
        columns={"SMALL_AREA_y": "SMALL_AREA_2016", "SMALL_AREA_x": "SMALL_AREA_2011"}
    )
    .loc[:, ["SMALL_AREA_2011", "SMALL_AREA_2016", "EDNAME"]]
)
small_areas_2011_vs_2011.to_csv(data_dir / "small_areas_2011_vs_2011.csv", index=False)

# %%
ireland_municipality_boundaries = gpd.read_file(
    data_dir / "Census2011_Admin_Counties_generalised20m"
)
dublin_municipalities = [
    "Dublin City",
    "South Dublin",
    "Fingal",
    "Dún Laoghaire-Rathdown",
]
dublin_municipality_boundaries = ireland_municipality_boundaries.query(
    f"COUNTYNAME in {dublin_municipalities}"
)[["COUNTYNAME", "geometry"]]
dublin_municipality_boundaries.to_file(
    data_dir / "Dublin_Census2011_Admin_Counties_generalised20m"
)

# %%
ireland_routing_key_boundaries = gpd.read_file(
    data_dir / "routingkeys_mi_itm_2016_09_29",
).to_crs(epsg=2157)
dublin_routing_key_boundaries = (
    get_geometries_within(
        ireland_routing_key_boundaries, dublin_boundary.to_crs(epsg=2157)
    )
    .assign(
        CountyName=lambda gdf: gdf["Descriptor"]
        .str.title()
        .str.replace(r"^(?!Dublin \d+)(.*)$", "Co. Dublin", regex=True)
    )
    .pipe(get_geometries_within, dublin_municipality_boundaries.to_crs(epsg=2157))
)
dublin_routing_key_boundaries.to_file(
    data_dir / "dublin_routing_boundaries.geojson",
    driver="GeoJSON",
)

# %%
