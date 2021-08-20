import geopandas as gpd
import pandas as pd
from prefect import task
from prefect.engine.serializers import PandasSerializer
from prefect.engine.results import LocalResult
from prefect_geopandas_serializer.serializers import GeoPandasSerializer


import functions
from globals import DATA_DIR

INPUT_FILENAMES = {
    "grid": "heatmap-download-version-nov-2020.parquet",
    "small_area_boundaries": "dublin_small_area_boundaries_in_routing_keys.parquet",
}


create_folder_structure = task(functions.create_folder_structure)
load_esb_substation_data = task(
    pd.read_csv,
    name="Load ESB Substation Data",
    checkpoint=True,
    target=INPUT_FILENAMES["grid"],
    result=LocalResult(
        dir=DATA_DIR / "external", serializer=PandasSerializer("parquet")
    ),
)
load_small_area_boundaries = task(
    gpd.read_file,
    name="Load Small Area Boundaries",
    checkpoint=True,
    target=INPUT_FILENAMES["small_area_boundaries"],
    result=LocalResult(
        dir=DATA_DIR / "external", serializer=GeoPandasSerializer("parquet")
    ),
)
convert_to_geodataframe = task(
    functions.convert_to_geodataframe, name="Convert to GeoDataFrame"
)
query = task(lambda df, query_str: df.query(query_str), name="Query")
