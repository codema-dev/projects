from dotenv import load_dotenv

load_dotenv(".prefect")  # load local prefect configuration prior to import!
from prefect import Flow

import tasks
from globals import DATA_DIR

URLS = {
    "dublin_boundary": "https://codema-dev.s3.eu-west-1.amazonaws.com/dublin_admin_county_boundaries.zip",
    "dublin_mv_index_ids": "https://codema-dev.s3.eu-west-1.amazonaws.com/esb_20210107_dublin_mv_index.csv",
}

DIRPATHS = {
    "gas": DATA_DIR / "external" / "DGN ITM-20210812T133007Z-001",
    "electricity": DATA_DIR / "external" / "ESBdata_20210107",
}

FILEPATHS = {
    "mv_index": DIRPATHS["electricity"] / "Ancillary Data" / "mv_index.dgn",
    "hv_network": DIRPATHS["electricity"] / "Ancillary Data" / "mv_index.dgn",
    "dublin_mv_index_ids": DATA_DIR / "external" / "esb_20210107_dublin_mv_index.csv",
    "dublin_boundary": DATA_DIR / "external" / "dublin_admin_county_boundaries.zip",
}

with Flow("Extract infrastructure small area line lengths") as flow:
    check_gas_data_exists = tasks.check_file_exists(DIRPATHS["gas"])
    check_electricity_data_exists = tasks.check_file_exists(DIRPATHS["electricity"])
    download_dublin_boundary = tasks.download_file(
        URLS["dublin_boundary"], FILEPATHS["dublin_boundary"]
    )
    download_dublin_mv_index_ids = tasks.download_file(
        URLS["dublin_mv_index_ids"], FILEPATHS["dublin_mv_index_ids"]
    )

    hv_network = tasks.read_network(FILEPATHS["hv_network"])
    mv_index = tasks.read_file(FILEPATHS["mv_index"], crs="EPSG:29903")
    dublin_mv_index_ids = tasks.read_csv(FILEPATHS["dublin_mv_index_ids"], header=None)
    dublin_boundary = tasks.read_file(FILEPATHS["dublin_boundary"], crs="EPSG:2157")

    dublin_mv_index = tasks.extract_mv_index(
        mv_index, on_column="Text", list_of_values=dublin_mv_index_ids
    )

    # set manual dependencies
    hv_network.set_upstream(check_electricity_data_exists)
    dublin_boundary.set_upstream(download_dublin_boundary)
    dublin_mv_index_ids.set_upstream(download_dublin_mv_index_ids)


flow.run()
