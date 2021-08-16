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
create_folder_structure = prefect.task(functions.create_folder_structure)
download_file = prefect.task(functions.download_file)
read_file = prefect.task(functions.read_file)
read_hv_network = prefect.task(
    functions.read_hv_network,
    target="ireland_hv_network.parquet",
    result=get_geopandas_result(DATA_DIR / "interim", filetype="parquet"),
    checkpoint=True,
)
read_mv_index_ids = prefect.task(functions.read_csv)
read_mvlv_network = prefect.task(
    functions.read_mvlv_network,
    target="dublin_region_mvlv_network.parquet",
    result=get_geopandas_result(DATA_DIR / "interim", filetype="parquet"),
    checkpoint=True,
)


extract_dublin_hv_network = prefect.task(
    functions.extract_in_boundary,
    target="dublin_hv_network.parquet",
    result=get_geopandas_result(DATA_DIR / "interim", filetype="parquet"),
    checkpoint=True,
    name="Extract Dublin HV Network",
)
extract_dublin_mvlv_network = prefect.task(
    functions.extract_in_boundary,
    target="dublin_mvlv_network.parquet",
    result=get_geopandas_result(DATA_DIR / "interim", filetype="parquet"),
    checkpoint=True,
    name="Extract Dublin MV-LV Network",
)
extract_lines = prefect.task(functions.query, name="Extract Lines")
cut_hv_lines_on_boundaries = prefect.task(
    functions.cut_lines_on_boundaries,
    target="dublin_hv_lines_cut.parquet",
    result=get_geopandas_result(DATA_DIR / "interim", filetype="parquet"),
    checkpoint=True,
    name="Cut HV Lines on Small Area Boundaries",
)
cut_mvlv_lines_on_boundaries = prefect.task(
    functions.cut_lines_on_boundaries,
    target="dublin_mvlv_lines_cut.parquet",
    result=get_geopandas_result(DATA_DIR / "interim", filetype="parquet"),
    checkpoint=True,
    name="Cut MV & LV Lines on Small Area Boundaries",
)

save_subset_to_gpkg = prefect.task(
    functions.save_subset_to_gpkg, name="Save Stations/Lines to GPKG"
)
