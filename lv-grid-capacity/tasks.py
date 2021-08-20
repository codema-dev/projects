import pandas as pd
from prefect import task
from prefect.engine.serializers import PandasSerializer
from prefect.engine.results import LocalResult

import functions
from globals import DATA_DIR

INPUT_FILEPATHS = {"grid": "heatmap-download-version-nov-2020.parquet"}

create_folder_structure = task(functions.create_folder_structure)
load_esb_substation_data = task(
    pd.read_csv,
    name="Load ESB Substation Data",
    checkpoint=True,
    target=INPUT_FILEPATHS["grid"],
    result=LocalResult(
        dir=DATA_DIR / "external", serializer=PandasSerializer("parquet")
    ),
)
convert_to_geodataframe = task(
    functions.convert_to_geodataframe, name="Convert to GeoDataFrame"
)
query = task(lambda df, query_str: df.query(query_str), name="Query")
