import geopandas as gpd
import pandas as pd
from prefect import task
from prefect.engine.serializers import PandasSerializer
from prefect.engine.results import LocalResult
from prefect_geopandas_serializer.serializers import GeoPandasSerializer


import functions
from globals import DATA_DIR

INPUT_FILENAMES = {
    "commercial": "valuation_office_dublin_april_2021.parquet",
    "residential": "bers_dublin_june_2021.parquet",
    "public_sector": "monitoring_and_reporting_dublin_21_1_20.parquet",
    "small_area_boundaries": "dublin_small_area_boundaries_in_routing_keys.parquet",
}

create_folder_structure = task(functions.create_folder_structure)
load_commercial = task(
    functions.load_parquet,
    name="Load Commercial Buildings",
)
load_residential = task(
    functions.load_parquet,
    name="Load Residential Buildings",
)
load_public_sector = task(
    functions.load_parquet,
    name="Load Public Sector Buildings",
)
load_small_area_boundaries = task(
    functions.load_file,
    name="Load Small Area Boundaries",
)

convert_to_geodataframe = task(
    functions.convert_to_geodataframe, name="Convert to GeoDataFrame"
)
extract_columns = task(lambda df, columns: df[columns], name="Extract Columns")
merge = task(lambda left, right, on: left.merge(right, on=on), name="Merge DataFrames")
sjoin = task(gpd.sjoin, name="Spatial Join")
amalgamate_to_electoral_district = task(
    functions.amalgamate_to_granularity, name="Amalgamate Column to Electoral District"
)
count_in_electoral_district = task(
    functions.count_in_granularity, name="Count Occurences in Electoral District"
)

save_to_csv = task(lambda df, filepath: df.to_csv(filepath), name="Save to CSV")
