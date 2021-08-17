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


def get_pandas_result(
    data_dir: Path,
    filetype: str,
    serialize_kwargs: Dict[str, Any],
    deserialize_kwargs: Dict[str, Any],
) -> results.LocalResult:
    return results.LocalResult(
        dir=data_dir,
        serializer=PandasSerializer(
            filetype,
            serialize_kwargs=serialize_kwargs,
            deserialize_kwargs=deserialize_kwargs,
        ),
    )


check_file_exists = prefect.task(functions.check_file_exists)
create_folder_structure = prefect.task(functions.create_folder_structure)
download_file = prefect.task(functions.download_file)
read_file = prefect.task(functions.read_file)
concatenate = prefect.task(pd.concat)
extract_dublin_centrelines = prefect.task(
    functions.extract_in_boundary,
    name="Extract Dublin Centrelines",
)
extract_dublin_leaderlines = prefect.task(
    functions.extract_in_boundary,
    name="Extract Dublin Leaderlines",
)
cut_centrelines_on_boundaries = prefect.task(
    functions.cut_lines_on_boundaries,
    name="Cut Centrelines on Small Area Boundaries",
)
cut_leaderlines_on_boundaries = prefect.task(
    functions.cut_lines_on_boundaries,
    name="Cut Leaderlines on Small Area Boundaries",
)
