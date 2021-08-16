from pathlib import Path
from typing import Dict
from typing import Any

import geopandas as gpd
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
download_file = prefect.task(functions.download_file)
read_file = prefect.task(functions.read_file)
read_hv_network = prefect.task(
    functions.read_hv_network,
    target="hv_network.parquet",
    result=get_geopandas_result(DATA_DIR / "interim", filetype="parquet"),
    checkpoint=True,
)
read_mv_index_ids = prefect.task(
    functions.read_csv,
    target="esb_20210107_dublin_mv_index.csv",
    result=get_pandas_result(
        DATA_DIR / "interim",
        filetype="csv",
        serialize_kwargs={"index": False, "header": None},
        deserialize_kwargs={"header": None, "squeeze": True},
    ),
    checkpoint=True,
)
read_mvlv_network = prefect.task(
    functions.read_mvlv_network,
    target="mvlv_network.parquet",
    result=get_geopandas_result(DATA_DIR / "interim", filetype="parquet"),
    checkpoint=True,
)
read_small_area_boundaries = prefect.task(gpd.read_file)
