from os import stat
from dotenv import load_dotenv

load_dotenv(".prefect")  # load local prefect configuration prior to import!
from prefect import Flow
from prefect import unmapped

import tasks
from globals import HERE
from globals import DATA_DIR

URLS = {
    "dublin_boundary": "https://codema-dev.s3.eu-west-1.amazonaws.com/dublin_admin_county_boundaries.zip",
    "dublin_mv_index_ids": "https://codema-dev.s3.eu-west-1.amazonaws.com/esb_20210107_dublin_mv_index.csv",
    "dublin_small_area_boundaries": "https://codema-dev.s3.eu-west-1.amazonaws.com/dublin_small_area_boundaries_in_routing_keys.gpkg",
}

DIRPATHS = {
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
    "dublin_mv_index_ids": DATA_DIR / "external" / "esb_20210107_dublin_mv_index.csv",
    "lv_single_phase": DATA_DIR / "processed" / "lv_single_phase_length.csv",
    "lv_three_phase": DATA_DIR / "processed" / "lv_three_phase_length.csv",
    "mv_single_phase": DATA_DIR / "processed" / "mv_single_phase_length.csv",
    "mv_three_phase": DATA_DIR / "processed" / "mv_three_phase_length.csv",
    "38kv_overhead": DATA_DIR / "processed" / "38kv_overhead_length.csv",
    "38kv_underground": DATA_DIR / "processed" / "38kv_underground_length.csv",
    "110kv_overhead": DATA_DIR / "processed" / "110kv_overhead_length.csv",
    "110kv_underground": DATA_DIR / "processed" / "110kv_underground_length.csv",
    "220kv_overhead": DATA_DIR / "processed" / "220kv_overhead_length.csv",
    "220kv_underground": DATA_DIR / "processed" / "220kv_underground_length.csv",
}
GEOFILEPATHS = {
    "mv_index": DIRPATHS["electricity"] / "Ancillary Data" / "mv_index.dgn",
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

MVLV_LINE_LEVELS = [1, 2, 10, 11]
HV_LINE_LEVELS = [21, 24, 31, 34, 41, 44]
STATION_LEVELS = [20, 30, 40]

with Flow("Extract infrastructure small area line lengths") as flow:
    create_folder_structure = tasks.create_folder_structure(DATA_DIR)

    check_electricity_data_exists = tasks.check_file_exists(
        DIRPATHS["electricity"], upstream_tasks=[create_folder_structure]
    )
    download_dublin_boundary = tasks.download_file(
        URLS["dublin_boundary"],
        GEOFILEPATHS["dublin_boundary"],
        upstream_tasks=[create_folder_structure],
    )
    download_dublin_mv_index_ids = tasks.download_file(
        URLS["dublin_mv_index_ids"],
        FILEPATHS["dublin_mv_index_ids"],
        upstream_tasks=[create_folder_structure],
    )
    download_dublin_small_area_boundaries = tasks.download_file(
        URLS["dublin_small_area_boundaries"],
        GEOFILEPATHS["dublin_small_area_boundaries"],
        upstream_tasks=[create_folder_structure],
    )

    dublin_boundary = tasks.read_file(
        GEOFILEPATHS["dublin_boundary"],
        crs="EPSG:2157",
        columns=["geometry"],
        upstream_tasks=[download_dublin_boundary],
    )
    hv_network = tasks.read_hv_network(
        DIRPATHS["hv_network"], upstream_tasks=[check_electricity_data_exists]
    )
    mv_index = tasks.read_file(
        GEOFILEPATHS["mv_index"],
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
        GEOFILEPATHS["dublin_small_area_boundaries"],
        upstream_tasks=[download_dublin_small_area_boundaries],
        crs="EPSG:2157",
    )

    dublin_hv_network = tasks.extract_dublin_hv_network(hv_network, dublin_boundary)
    dublin_mvlv_network = tasks.extract_dublin_mvlv_network(
        dublin_region_mvlv_network, dublin_boundary
    )

    station_query_strs = [f"Level == {level}" for level in STATION_LEVELS]
    stations = tasks.query.map(
        unmapped(dublin_hv_network), query_str=station_query_strs
    )

    mvlv_lines = tasks.extract_lines(
        dublin_mvlv_network, "Level == 1 or Level == 2 or Level == 10 or Level == 11"
    )
    hv_lines = tasks.extract_lines(
        dublin_hv_network,
        "Level == 21 or Level == 24 or Level == 31 or Level == 34 or Level == 41 or Level == 44",
    )
    small_area_mvlv_lines = tasks.cut_mvlv_lines_on_boundaries(
        mvlv_lines, dublin_small_area_boundaries
    )
    small_area_hv_lines = tasks.cut_hv_lines_on_boundaries(
        hv_lines, dublin_small_area_boundaries
    )

    mvlv_line_query_strs = [f"Level == {level}" for level in MVLV_LINE_LEVELS]
    small_area_mvlv_lines_by_voltage = tasks.query.map(
        unmapped(small_area_mvlv_lines), query_str=mvlv_line_query_strs
    )
    small_area_mvlv_line_lengths = tasks.measure_small_area_line_lengths.map(
        small_area_mvlv_lines_by_voltage, boundary_column_name=unmapped("small_area")
    )

    hv_line_query_strs = [f"Level == {level}" for level in HV_LINE_LEVELS]
    small_area_hv_lines_by_voltage = tasks.query.map(
        unmapped(small_area_hv_lines), query_str=hv_line_query_strs
    )
    small_area_hv_line_lengths = tasks.measure_small_area_line_lengths.map(
        small_area_hv_lines_by_voltage, boundary_column_name=unmapped("small_area")
    )

    tasks.save_to_gpkg.map(
        gdf=stations,
        filepath=[
            GEOFILEPATHS["38kv_stations"],
            GEOFILEPATHS["110kv_stations"],
            GEOFILEPATHS["220kv_stations"],
        ],
    )
    tasks.save_to_csv.map(
        df=small_area_mvlv_line_lengths,
        filepath=[
            FILEPATHS["lv_three_phase"],
            FILEPATHS["lv_single_phase"],
            FILEPATHS["mv_three_phase"],
            FILEPATHS["mv_single_phase"],
        ],
    )
    tasks.save_to_gpkg.map(
        gdf=small_area_mvlv_lines_by_voltage,
        filepath=[
            GEOFILEPATHS["lv_three_phase"],
            GEOFILEPATHS["lv_single_phase"],
            GEOFILEPATHS["mv_three_phase"],
            GEOFILEPATHS["mv_single_phase"],
        ],
    )
    tasks.save_to_csv.map(
        df=small_area_hv_line_lengths,
        filepath=[
            FILEPATHS["38kv_overhead"],
            FILEPATHS["38kv_underground"],
            FILEPATHS["110kv_overhead"],
            FILEPATHS["110kv_underground"],
            FILEPATHS["220kv_overhead"],
            FILEPATHS["220kv_underground"],
        ],
    )
    tasks.save_to_gpkg.map(
        gdf=small_area_hv_lines_by_voltage,
        filepath=[
            GEOFILEPATHS["38kv_overhead"],
            GEOFILEPATHS["38kv_underground"],
            GEOFILEPATHS["110kv_overhead"],
            GEOFILEPATHS["110kv_underground"],
            GEOFILEPATHS["220kv_overhead"],
            GEOFILEPATHS["220kv_underground"],
        ],
    )

state = flow.run()

flow.visualize(flow_state=state, filename=HERE / "flow", format="png")
