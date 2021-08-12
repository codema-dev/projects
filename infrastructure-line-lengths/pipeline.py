from pathlib import Path

from prefect import Flow

import tasks

HERE = Path(__name__).parent
DATA_DIR = HERE / "data"

DIRPATHS = {
    "gas": DATA_DIR / "external" / "DGN ITM-20210812T133007Z-001",
    "electricity": DATA_DIR / "external" / "ESBdata_20210107",
}

FILEPATHS = {
    "hv_network": DIRPATHS["electricity"] / "Dig Request Style" / "HV Data",
}

with Flow("Extract infrastructure small area line lengths") as flow:
    check_gas_data_exists = tasks.check_file_exists(DIRPATHS["gas"])
    check_electricity_data_exists = tasks.check_file_exists(DIRPATHS["electricity"])

    hv_network = tasks.read_network(FILEPATHS["hv_network"])

    # set manual dependencies
    hv_network.set_upstream(check_electricity_data_exists)

flow.run()
