from dotenv import load_dotenv

load_dotenv(".prefect")  # load local prefect configuration prior to import!
from prefect import Flow

import tasks
from globals import HERE
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
    "38kv_stations": DATA_DIR / "processed" / "38kv_stations.gpkg",
    "110kv_stations": DATA_DIR / "processed" / "110kv_stations.gpkg",
    "220kv_stations": DATA_DIR / "processed" / "220kv_stations.gpkg",
    "400kv_stations": DATA_DIR / "processed" / "400kv_stations.gpkg",
    "lv_single_phase": DATA_DIR / "processed" / "lv_single_phase.gpkg",
    "lv_three_phase": DATA_DIR / "processed" / "lv_three_phase.gpkg",
    "mv_single_phase": DATA_DIR / "processed" / "mv_single_phase.gpkg",
    "mv_three_phase": DATA_DIR / "processed" / "mv_three_phase.gpkg",
    "38kv_overhead": DATA_DIR / "processed" / "38kv_overhead.gpkg",
    "38kv_underground": DATA_DIR / "processed" / "38kv_underground.gpkg",
    "110kv_overhead": DATA_DIR / "processed" / "110kv_overhead.gpkg",
    "110kv_underground": DATA_DIR / "processed" / "110kv_underground.gpkg",
    "220kv_overhead": DATA_DIR / "processed" / "220kv_overhead.gpkg",
    "220kv_underground": DATA_DIR / "processed" / "220kv_underground.gpkg",
}

with Flow("Extract infrastructure small area line lengths") as flow:
    create_folder_structure = tasks.create_folder_structure(DATA_DIR)

    check_gas_data_exists = tasks.check_file_exists(DIRPATHS["gas"])
    check_electricity_data_exists = tasks.check_file_exists(
        DIRPATHS["electricity"], upstream_tasks=[create_folder_structure]
    )
    download_dublin_boundary = tasks.download_file(
        URLS["dublin_boundary"],
        FILEPATHS["dublin_boundary"],
        upstream_tasks=[create_folder_structure],
    )
    download_dublin_mv_index_ids = tasks.download_file(
        URLS["dublin_mv_index_ids"],
        FILEPATHS["dublin_mv_index_ids"],
        upstream_tasks=[create_folder_structure],
    )
    download_dublin_small_area_boundaries = tasks.download_file(
        URLS["dublin_small_area_boundaries"],
        FILEPATHS["dublin_small_area_boundaries"],
        upstream_tasks=[create_folder_structure],
    )

    dublin_boundary = tasks.read_file(
        FILEPATHS["dublin_boundary"],
        crs="EPSG:2157",
        columns=["geometry"],
        upstream_tasks=[download_dublin_boundary],
    )
    hv_network = tasks.read_hv_network(
        DIRPATHS["hv_network"], upstream_tasks=[check_electricity_data_exists]
    )
    mv_index = tasks.read_file(
        FILEPATHS["mv_index"],
        crs="EPSG:29903",
        upstream_tasks=[check_electricity_data_exists],
    )
    dublin_mv_index_ids = tasks.read_mv_index_ids(
        FILEPATHS["dublin_mv_index_ids"],
        header=None,
        upstream_tasks=[download_dublin_mv_index_ids],
    )
    dublin_region_mvlv_network = tasks.read_mvlv_network(
        DIRPATHS["mvlv_network"],
        ids=dublin_mv_index_ids,
        upstream_tasks=[check_electricity_data_exists],
    )
    dublin_small_area_boundaries = tasks.read_file(
        FILEPATHS["dublin_small_area_boundaries"],
        upstream_tasks=[download_dublin_small_area_boundaries],
        crs="EPSG:2157",
    )

    dublin_hv_network = tasks.extract_dublin_hv_network(hv_network, dublin_boundary)
    dublin_mvlv_network = tasks.extract_dublin_mvlv_network(
        dublin_region_mvlv_network, dublin_boundary
    )

    tasks.save_subset_to_gpkg(
        gdf=dublin_hv_network,
        query_str="Level == 20",
        filepath=FILEPATHS["38kv_stations"],
    )
    tasks.save_subset_to_gpkg(
        gdf=dublin_hv_network,
        query_str="Level == 30",
        filepath=FILEPATHS["110kv_stations"],
    )
    tasks.save_subset_to_gpkg(
        gdf=dublin_hv_network,
        query_str="Level == 40",
        filepath=FILEPATHS["220kv_stations"],
    )

    mvlv_lines = tasks.query(
        dublin_mvlv_network, "Level == 1 or Level == 2 or Level == 10 or Level == 11"
    )
    hv_lines = tasks.query(
        dublin_hv_network,
        "Level == 21 or Level == 24 or Level == 31 or Level == 34 or Level == 41 or Level == 44",
    )
    mvlv_lines_cut = tasks.cut_mvlv_lines_on_boundaries(
        mvlv_lines, dublin_small_area_boundaries
    )
    hv_lines_cut = tasks.cut_hv_lines_on_boundaries(
        hv_lines, dublin_small_area_boundaries
    )

    tasks.save_subset_to_gpkg(
        gdf=mvlv_lines_cut, query_str="Level == 1", filepath=FILEPATHS["lv_three_phase"]
    )
    tasks.save_subset_to_gpkg(
        gdf=mvlv_lines_cut,
        query_str="Level == 2",
        filepath=FILEPATHS["lv_single_phase"],
    )
    tasks.save_subset_to_gpkg(
        gdf=mvlv_lines_cut,
        query_str="Level == 10",
        filepath=FILEPATHS["mv_three_phase"],
    )
    tasks.save_subset_to_gpkg(
        gdf=mvlv_lines_cut,
        query_str="Level == 11",
        filepath=FILEPATHS["mv_single_phase"],
    )
    tasks.save_subset_to_gpkg(
        gdf=hv_lines_cut, query_str="Level == 21", filepath=FILEPATHS["38kv_overhead"]
    )
    tasks.save_subset_to_gpkg(
        gdf=hv_lines_cut,
        query_str="Level == 24",
        filepath=FILEPATHS["38kv_underground"],
    )
    tasks.save_subset_to_gpkg(
        gdf=hv_lines_cut, query_str="Level == 31", filepath=FILEPATHS["110kv_overhead"]
    )
    tasks.save_subset_to_gpkg(
        gdf=hv_lines_cut,
        query_str="Level == 34",
        filepath=FILEPATHS["110kv_underground"],
    )
    tasks.save_subset_to_gpkg(
        gdf=hv_lines_cut, query_str="Level == 41", filepath=FILEPATHS["220kv_overhead"]
    )
    tasks.save_subset_to_gpkg(
        gdf=hv_lines_cut,
        query_str="Level == 44",
        filepath=FILEPATHS["220kv_underground"],
    )

state = flow.run()

flow.visualize(flow_state=state, filename=HERE / "flow", format="png")
