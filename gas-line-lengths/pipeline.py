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

PRESSURES = [
    "75 mbar (Low Pressure)",
    "4 bar (Medium Pressure)",
    "25 mbar (Low Pressure)",
    "70 bar (High Pressure)",
    "19 bar (High Pressure)",
    "85 bar (High Pressure)",
    "700 mbar (Medium Pressure)",
    "100 mbar (Low Pressure)",
    "40 bar (High Pressure)",
    "2 bar (Medium Pressure)",
    "75 bar (High Pressure)",
    "145 bar (High Pressure)",
]
DIAMETERS = [
    "125mm",
    "90mm",
    "180mm",
    "315mm",
    "63mm",
    "100mm",
    "200mm",
    "250mm",
    "32mm",
    "150mm",
    "20mm",
    "2 In",
    "3 In",
    "4 In",
    "6 In",
    "8 In",
    "25mm",
    "10 In",
    "300mm",
    "50mm",
    "16 In",
    "400mm",
    "1 In",
    "80mm",
    "900mm",
    "450mm",
    "750mm",
    "600mm",
    "500mm",
    "12 In",
    "75mm",
    "18 In",
    "24 In",
    "1.5 In",
]

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
    download_dublin_small_area_boundaries = tasks.download_file(
        URLS["dublin_small_area_boundaries"],
        FILEPATHS["dublin_small_area_boundaries"],
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
    dublin_boundary = tasks.read_file(
        FILEPATHS["dublin_boundary"],
        crs="EPSG:2157",
        columns=["geometry"],
        upstream_tasks=[download_dublin_boundary],
    )
    dublin_small_area_boundaries = tasks.read_file(
        FILEPATHS["dublin_small_area_boundaries"],
        upstream_tasks=[download_dublin_small_area_boundaries],
        crs="EPSG:2157",
    )

    centrelines = tasks.concatenate(list_of_centrelines)

    dublin_centrelines = tasks.extract_lines_in_dublin_boundary(
        centrelines, dublin_boundary
    )
    small_area_centrelines = tasks.cut_lines_on_boundaries(
        dublin_centrelines, dublin_small_area_boundaries
    )

    diameter_queries = [f"diameter == '{diameter}'" for diameter in DIAMETERS]
    centrelines_by_diameter = tasks.query.map(
        unmapped(small_area_centrelines), diameter_queries
    )
    tasks.save_to_gpkg.map(
        centrelines_by_diameter,
        filepath=[
            DATA_DIR / "processed" / f"{diameter} - lines.gpkg"
            for diameter in DIAMETERS
        ],
    )
    small_area_line_lengths_by_diameter = tasks.measure_small_area_line_lengths.map(
        centrelines_by_diameter, boundary_column_name=unmapped("small_area")
    )
    tasks.save_to_csv.map(
        small_area_line_lengths_by_diameter,
        filepath=[
            DATA_DIR / "processed" / f"{diameter} - small_area.csv"
            for diameter in DIAMETERS
        ],
    )

    pressure_queries = [f"pressure == '{pressure}'" for pressure in PRESSURES]
    centrelines_by_pressure = tasks.query.map(
        unmapped(small_area_centrelines), pressure_queries
    )
    tasks.save_to_gpkg.map(
        centrelines_by_pressure,
        filepath=[
            DATA_DIR / "processed" / f"{pressure} - lines.gpkg"
            for pressure in PRESSURES
        ],
    )
    small_area_line_lengths_by_pressure = tasks.measure_small_area_line_lengths.map(
        centrelines_by_pressure, boundary_column_name=unmapped("small_area")
    )
    tasks.save_to_csv.map(
        small_area_line_lengths_by_pressure,
        filepath=[
            DATA_DIR / "processed" / f"{pressure} - small_area.csv"
            for pressure in PRESSURES
        ],
    )

state = flow.run()
flow.visualize(flow_state=state, filename=HERE / "flow", format="png")
