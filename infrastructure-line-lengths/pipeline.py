from pathlib import Path

from prefect import Flow

import tasks

DATA_DIR = Path("data")

filepaths = {
    "gas": DATA_DIR / "external" / "DGN ITM-20210812T133007Z-001",
    "electricity": DATA_DIR / "external" / "ESBdata_20210107",
}

with Flow("Extract infrastructure small area line lengths") as flow:
    check_gas_data_exists = tasks.check_file_exists(filepaths["gas"])
    check_electricity_data_exists = tasks.check_file_exists(filepaths["electricity"])
