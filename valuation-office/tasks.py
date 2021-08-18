from os import getenv
from pathlib import Path

import prefect
from prefect.engine import results
from prefect.engine import serializers

import functions
from globals import BASE_DIR
from globals import DATA_DIR
from prefect_geopandas_serializer.serializers import GeoPandasSerializer


def check_if_s3_keys_are_defined() -> None:
    message = f"""

        Please create a .env file
        
        In this directory: {BASE_DIR.resolve()}

        With the following contents:
        
        AWS_ACCESS_KEY_ID=YOUR_KEY
        AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
    """
    assert getenv("AWS_ACCESS_KEY_ID") is not None, message
    assert getenv("AWS_SECRET_ACCESS_KEY") is not None, message


def get_json_result(data_dir: Path) -> results.LocalResult:
    return results.LocalResult(
        dir=data_dir,
        serializer=serializers.JSONSerializer(),
    )


def get_parquet_result(data_dir: Path) -> results.LocalResult:
    return results.LocalResult(
        dir=data_dir,
        serializer=serializers.PandasSerializer("parquet"),
    )


def get_geoparquet_result(data_dir: Path) -> results.LocalResult:
    return results.LocalResult(
        dir=data_dir,
        serializer=GeoPandasSerializer("parquet"),
    )


load_valuation_office = prefect.task(
    functions.read_parquet,
    target="raw_valuation_office.parquet",
    checkpoint=True,
    result=get_parquet_result(DATA_DIR / "external"),
    name="Load Dublin Valuation Office",
)

load_benchmark_uses = prefect.task(
    functions.read_benchmark_uses,
    target="benchmark_uses.json",
    checkpoint=True,
    result=get_json_result(DATA_DIR / "external"),
    name="Load Valuation Office Benchmark Uses",
)

load_benchmarks = prefect.task(
    functions.read_csv,
    target="benchmarks.parquet",
    checkpoint=True,
    result=get_parquet_result(DATA_DIR / "external"),
    name="Load Energy Benchmarks",
)

load_small_area_boundaries = prefect.task(
    functions.read_file,
    target="small_area_boundaries.parquet",
    checkpoint=True,
    result=get_geoparquet_result(DATA_DIR / "external"),
    name="Load Dublin Small Area Boundaries",
)

link_valuation_office_to_small_areas = prefect.task(
    functions.link_valuation_office_to_small_areas,
    target="valuation_office_small_areas.parquet",
    checkpoint=True,
    result=get_geoparquet_result(DATA_DIR / "interim"),
    name="Link Valuation Office to Small Area Boundaries",
)

link_valuation_office_to_benchmarks = prefect.task(
    functions.link_valuation_office_to_benchmarks,
)
apply_benchmarks_to_valuation_office_floor_areas = prefect.task(
    functions.apply_benchmarks_to_valuation_office_floor_areas
)
estimate_heat_demand = prefect.task(functions.estimate_heat_demand)

to_csv = prefect.task(functions.to_csv, name="Save to CSV")
