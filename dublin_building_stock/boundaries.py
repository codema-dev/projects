import geopandas as gpd
from loguru import logger

from dublin_building_stock.spatial_operations import (
    get_geometries_within,
)


def create_dublin_small_area_boundaries(data_dir, dublin_boundary):

    filenames = [
        "Census2011_Small_Areas_generalised20m",
        "Small_Areas_Ungeneralised_-_OSi_National_Statistical_Boundaries_-_2015-shp",
    ]
    filepaths = [data_dir / filename for filename in filenames]
    for filepath in filepaths:
        use_columns = ["SMALL_AREA", "EDNAME", "geometry"]
        ireland_small_area_boundaries = gpd.read_file(filepath)[use_columns]
        dublin_small_area_boundaries = get_geometries_within(
            ireland_small_area_boundaries.to_crs(epsg=2157),
            dublin_boundary.to_crs(epsg=2157),
        )
        output_filepath = filepath.parent / ("Dublin_" + filepath.stem)
        dublin_small_area_boundaries.to_file(output_filepath)
