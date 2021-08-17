from pathlib import Path
from typing import Dict
from typing import Any

import geopandas as gpd
import pandas as pd
import prefect
from prefect.engine import results
from prefect.engine.serializers import PandasSerializer

import functions
from globals import DATA_DIR

from prefect_geopandas_serializer.serializers import GeoPandasSerializer


def get_geopandas_result(data_dir: Path, filetype: str) -> results.LocalResult:
    return results.LocalResult(
        dir=data_dir,
        serializer=GeoPandasSerializer(filetype),
    )


check_file_exists = prefect.task(functions.check_file_exists)
create_folder_structure = prefect.task(functions.create_folder_structure)
download_file = prefect.task(functions.download_file)
read_file = prefect.task(functions.read_file)

concatenate = prefect.task(pd.concat)
extract_lines_in_dublin_boundary = prefect.task(
    functions.extract_in_boundary,
    name="Extract Lines in Dublin Boundary",
    checkpoint=True,
    result=get_geopandas_result(DATA_DIR / "interim", filetype="parquet"),
    target="centrelines_in_dublin_boundary.parquet",
)
cut_lines_on_boundaries = prefect.task(
    functions.cut_lines_on_boundaries,
    name="Cut Lines on Small Area Boundaries",
    checkpoint=True,
    result=get_geopandas_result(DATA_DIR / "interim", filetype="parquet"),
    target="centrelines_in_dublin_small_areas.parquet",
)

save_to_gpkg = prefect.task(functions.save_subset_to_gpkg, name="Save to GPKG")
