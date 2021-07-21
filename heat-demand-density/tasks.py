from pathlib import Path

import prefect
from prefect.engine import results
from prefect.engine import serializers

import functions
from globals import DATA_DIR
from serializers import GeoPandasSerializer


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

load_bers = prefect.task(
    functions.read_parquet,
    target="small_area_bers.parquet",
    checkpoint=True,
    result=get_parquet_result(DATA_DIR / "external"),
    name="Load Dublin Small Area BERs",
)

load_benchmark_uses = prefect.task(
    functions.read_benchmark_uses,
    target="benchmark_uses.json",
    checkpoint=True,
    result=get_json_result(DATA_DIR / "external"),
    name="Load Valuation Office Benchmark Uses",
)

load_benchmarks = prefect.task(
    functions.read_excel,
    target="benchmarks.parquet",
    checkpoint=True,
    result=get_parquet_result(DATA_DIR / "external"),
    name="Load Energy Benchmarks",
)

load_small_area_boundaries = prefect.task(
    functions.read_geoparquet,
    target="small_area_boundaries.parquet",
    checkpoint=True,
    result=get_geoparquet_result(DATA_DIR / "external"),
    name="Load Dublin Small Area Boundaries",
)

load_local_authority_boundaries = prefect.task(
    functions.read_zipped_shp,
    target="local_authority_boundaries.parquet",
    checkpoint=True,
    result=get_geoparquet_result(DATA_DIR / "external"),
    name="Load Dublin Local Authority Boundaries",
)

link_small_areas_to_local_authorities = prefect.task(
    functions.link_small_areas_to_local_authorities,
    name="Link Each Small Areas to their Corresponding Local Authority",
)

apply_benchmarks_to_valuation_office_floor_areas = prefect.task(
    functions.apply_benchmarks_to_valuation_office_floor_areas,
    name="Apply Energy Benchmarks to Valuation Office Floor Areas",
)

link_valuation_office_to_small_areas = prefect.task(
    functions.link_valuation_office_to_small_areas,
    target="valuation_office_small_areas.parquet",
    checkpoint=True,
    result=get_geoparquet_result(DATA_DIR / "interim"),
    name="Link Valuation Office to Small Area Boundaries",
)

extract_residential_heat_demand = prefect.task(
    functions.extract_residential_heat_demand,
    name="Extract DEAP Residential Heat Demand",
)

amalgamate_heat_demands_to_small_areas = prefect.task(
    functions.amalgamate_heat_demands_to_small_areas,
    target="dublin_small_area_demand_mwh_per_y.parquet",
    checkpoint=True,
    result=get_parquet_result("{parameters['data_dir']}/processed"),
    name="Amalgamate Heat Demands to Small Areas",
)

convert_from_mwh_per_y_to_tj_per_km2 = prefect.task(
    functions.convert_from_mwh_per_y_to_tj_per_km2,
    target="dublin_small_area_demand_tj_per_km2.parquet",
    checkpoint=True,
    result=get_parquet_result(DATA_DIR / "processed"),
    name="Convert Heat Demands from MWh/year to TJ/km2",
)

map_demand = prefect.task(
    functions.map_demand,
    name="Map Small Area Heat Demands",
)
