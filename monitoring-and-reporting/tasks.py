from os import getenv
from pathlib import Path

import prefect
from prefect.engine import results
from prefect.engine import serializers

import functions
from globals import BASE_DIR
from globals import DATA_DIR


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


def get_csv_result(data_dir: Path) -> results.LocalResult:
    return results.LocalResult(
        dir=data_dir,
        serializer=serializers.PandasSerializer("csv"),
    )


load_monitoring_and_reporting = prefect.task(
    functions.read_excel_sheets,
    target="raw_monitoring_and_reporting.cloudpickle",
    checkpoint=True,
    result=results.LocalResult(DATA_DIR / "external"),
    name="Load Raw Dublin Monitoring & Reporting",
)


clean_mprn = prefect.task(functions.clean, name="Clean MPRN")
clean_gprn = clean_mprn

merge = prefect.task(functions.merge_sheets, name="Merge MPRN & GPRN")
pivot_to_one_column_per_year = prefect.task(
    functions.pivot_to_one_column_per_year,
    checkpoint=True,
    target="monitoring_and_reporting.csv",
    result=get_csv_result(DATA_DIR / "processed"),
    name="Pivot data to one column per year",
)
