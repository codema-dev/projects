from pathlib import Path

import geopandas as gpd
import prefect
from prefect.engine import results

import functions
from globals import DATA_DIR

from prefect_geopandas_serializer.serializers import GeoPandasSerializer


def get_geoparquet_result(data_dir: Path) -> results.LocalResult:
    return results.LocalResult(
        dir=data_dir,
        serializer=GeoPandasSerializer("parquet"),
    )


check_file_exists = prefect.task(functions.check_file_exists)
download_file = prefect.task(functions.download_file)
read_file = prefect.task(gpd.read_file)
read_network = prefect.task(
    functions.read_network,
    target="hv_network.parquet",
    result=get_geoparquet_result(DATA_DIR / "interim"),
    checkpoint=True,
)
