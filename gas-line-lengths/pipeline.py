from dotenv import load_dotenv

load_dotenv(".prefect")  # load local prefect configuration prior to import!
from prefect import Flow
from prefect import unmapped
from prefect.tasks.core.collections import List

import tasks
from globals import HERE
from globals import DATA_DIR

URLS = {
    "dublin_boundary": "https://codema-dev.s3.eu-west-1.amazonaws.com/dublin_admin_county_boundaries.zip",
    "dublin_small_area_boundaries": "https://codema-dev.s3.eu-west-1.amazonaws.com/dublin_small_area_boundaries_in_routing_keys.gpkg",
}

DIRPATHS = {"gas": DATA_DIR / "external" / "Tx and Dx"}
FILEPATHS = {
    "dublin_boundary": DATA_DIR / "external" / "dublin_admin_county_boundaries.zip",
    "dublin_small_area_boundaries": DATA_DIR
    / "external"
    / "dublin_small_area_boundaries_in_routing_keys.gpkg",
    "lp_centrelines": DIRPATHS["gas"] / "SHP ITM" / "2020Q4_LP_SHP_ITM_Centreline.shp",
    "lp_leaderlines": DIRPATHS["gas"] / "SHP ITM" / "2020Q4_LP_SHP_ITM_Leaderline.shp",
    "mp_centrelines": DIRPATHS["gas"] / "SHP ITM" / "2020Q4_MP_SHP_ITM_Centreline.shp",
    "mp_leaderlines": DIRPATHS["gas"] / "SHP ITM" / "2020Q4_MP_SHP_ITM_Leaderline.shp",
    "hp_centrelines": DIRPATHS["gas"] / "SHP ITM" / "2020Q4_HP_SHP_ITM_Centreline.shp",
    "hp_leaderlines": DIRPATHS["gas"] / "SHP ITM" / "2020Q4_HP_SHP_ITM_Leaderline.shp",
}

with Flow("Extract infrastructure small area line lengths") as flow:
    create_folder_structure = tasks.create_folder_structure(DATA_DIR)
    check_gas_data_exists = tasks.check_file_exists(
        DIRPATHS["gas"], upstream_tasks=[create_folder_structure]
    )
    download_dublin_boundary = tasks.download_file(
        URLS["dublin_boundary"],
        FILEPATHS["dublin_boundary"],
        upstream_tasks=[create_folder_structure],
    )

    list_of_centrelines = tasks.read_file.map(
        [
            FILEPATHS["lp_centrelines"],
            FILEPATHS["mp_centrelines"],
            FILEPATHS["hp_centrelines"],
        ],
        crs=unmapped("EPSG:2157"),
    ).set_upstream(check_gas_data_exists)
    list_of_leaderlines = tasks.read_file.map(
        [
            FILEPATHS["lp_leaderlines"],
            FILEPATHS["mp_leaderlines"],
            FILEPATHS["hp_leaderlines"],
        ],
        crs=unmapped("EPSG:2157"),
    ).set_upstream(check_gas_data_exists)
    dublin_boundary = tasks.read_file(
        FILEPATHS["dublin_boundary"],
        crs="EPSG:2157",
        columns=["geometry"],
    ).set_upstream(download_dublin_boundary)

    centrelines = tasks.concatenate(list_of_centrelines)
    leaderlines = tasks.concatenate(list_of_leaderlines)

    dublin_centrelines = tasks.extract_dublin_centrelines(centrelines, dublin_boundary)
    dublin_leaderlines = tasks.extract_dublin_leaderlines(leaderlines, dublin_boundary)


state = flow.run()
flow.visualize(flow_state=state, filename=HERE / "flow", format="png")
