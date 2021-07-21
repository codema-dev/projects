from configparser import ConfigParser
import os
from pathlib import Path
from typing import Any
from typing import Dict

import dotenv
from fsspec.registry import filesystem

dotenv.load_dotenv(".prefect")  # local local prefect configuration
import prefect
from prefect.engine import results
from prefect.engine import serializers

import tasks

HERE = Path(__name__).parent
CONFIG = ConfigParser()
CONFIG.read(HERE / "config.ini")
DATA_DIR = HERE / "data"
dotenv.load_dotenv()  # load s3 credentials


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


def estimate_heat_demand_density(
    config: ConfigParser = CONFIG, data_dir: Path = DATA_DIR
):
    filepaths: Dict[str, Any] = {
        "valuation_office": {
            "dcc": data_dir / "raw" / "dcc.ods",
            "dlrcc": data_dir / "raw" / "dlrcc.ods",
            "fcc": data_dir / "raw" / "fcc.ods",
            "sdcc": data_dir / "raw" / "sdcc.ods",
        },
    }

    load_valuation_office = prefect.task(
        tasks.read_parquet,
        target="raw_valuation_office.parquet",
        checkpoint=True,
        result=get_parquet_result(data_dir / "interim"),
    )
    load_bers = prefect.task(
        tasks.read_parquet,
        target="small_area_bers.parquet",
        checkpoint=True,
        result=get_parquet_result(data_dir / "external"),
    )
    load_benchmark_uses = prefect.task(
        tasks.read_benchmark_uses,
        target="benchmark_uses.json",
        checkpoint=True,
        result=get_json_result(data_dir / "external"),
    )
    load_benchmarks = prefect.task(
        tasks.read_excel,
        target="benchmarks.parquet",
        checkpoint=True,
        result=get_parquet_result(data_dir / "external"),
    )

    apply_benchmarks_to_valuation_office_floor_areas = prefect.task(
        tasks.apply_benchmarks_to_valuation_office_floor_areas
    )

    with prefect.Flow("Estimate Heat Demand Density") as flow:
        # Extract
        valuation_office = load_valuation_office(
            url=config["valuation_office"]["url"], filesystem_name="s3"
        )
        bers = load_bers(url=config["bers"]["url"], filesystem_name="s3")
        benchmark_uses = load_benchmark_uses(
            url=config["benchmark_uses"]["url"], filesystem_name="s3"
        )
        benchmarks = load_benchmarks(
            url=config["benchmarks"]["url"], filesystem_name="s3"
        )

        # Transform
        non_residential_demand = apply_benchmarks_to_valuation_office_floor_areas(
            valuation_office=valuation_office,
            benchmark_uses=benchmark_uses,
            benchmarks=benchmarks,
        )

    ## Run flow!
    from prefect.utilities.debug import raise_on_exception

    with raise_on_exception():
        flow.run()


if __name__ == "__main__":
    estimate_heat_demand_density()
