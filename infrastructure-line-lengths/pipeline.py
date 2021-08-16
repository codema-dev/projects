from dotenv import load_dotenv

load_dotenv(".prefect")  # load local prefect configuration prior to import!
from prefect import Flow

import tasks
from globals import DATA_DIR

URLS = {
    "dublin_boundary": "https://codema-dev.s3.eu-west-1.amazonaws.com/dublin_admin_county_boundaries.zip",
    "dublin_mv_index_ids": "https://codema-dev.s3.eu-west-1.amazonaws.com/esb_20210107_dublin_mv_index.csv",
    "dublin_small_area_boundaries": "https://codema-dev.s3.eu-west-1.amazonaws.com/dublin_small_area_boundaries_in_routing_keys.gpkg",
}

DIRPATHS = {
    "gas": DATA_DIR / "external" / "DGN ITM-20210812T133007Z-001",
    "electricity": DATA_DIR / "external" / "ESBdata_20210107",
    "hv_network": DATA_DIR
    / "external"
    / "ESBdata_20210107"
    / "Dig Request Style"
    / "HV Data",
    "mvlv_network": DATA_DIR
    / "external"
    / "ESBdata_20210107"
    / "Dig Request Style"
    / "MV-LV Data",
}

FILEPATHS = {
    "mv_index": DIRPATHS["electricity"] / "Ancillary Data" / "mv_index.dgn",
    "dublin_mv_index_ids": DATA_DIR / "external" / "esb_20210107_dublin_mv_index.csv",
    "dublin_boundary": DATA_DIR / "external" / "dublin_admin_county_boundaries.zip",
    "dublin_small_area_boundaries": DATA_DIR
    / "external"
    / "dublin_small_area_boundaries_in_routing_keys.gpkg",
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
    download_dublin_small_area_boundaries = tasks.download_file(
        URLS["dublin_small_area_boundaries"], FILEPATHS["dublin_small_area_boundaries"]
    )

    hv_network = tasks.read_hv_network(DIRPATHS["hv_network"])
    mv_index = tasks.read_file(FILEPATHS["mv_index"], crs="EPSG:29903")
    dublin_mv_index_ids = tasks.read_mv_index_ids(
        FILEPATHS["dublin_mv_index_ids"], header=None
    )
    mvlv_network = tasks.read_mvlv_network(
        DIRPATHS["mvlv_network"], ids=dublin_mv_index_ids
    )
    dublin_boundary = tasks.read_file(FILEPATHS["dublin_boundary"], crs="EPSG:2157")
    dublin_small_area_boundaries = tasks.read_small_area_boundaries(
        FILEPATHS["dublin_small_area_boundaries"]
    )

    mvlv_lines = tasks.query(
        mvlv_network, "Level == 1 or Level == 2 or Level == 10 or Level == 11"
    )
    hv_lines = tasks.query(hv_network, "Level == 20 or Level == 30 or Level == 40")

    # set manual dependencies
    hv_network.set_upstream(check_electricity_data_exists)
    mvlv_network.set_upstream(check_electricity_data_exists)
    dublin_boundary.set_upstream(download_dublin_boundary)
    dublin_mv_index_ids.set_upstream(download_dublin_mv_index_ids)
    dublin_small_area_boundaries.set_upstream(download_dublin_small_area_boundaries)

flow.run()
