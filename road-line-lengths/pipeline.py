from dotenv import load_dotenv

load_dotenv(".prefect")  # must load before importing prefect!
from prefect import Flow

from globals import DATA_DIR, HERE
import tasks

DAY = 19
MONTH = 8
YEAR = 2021

URLS = {
    "dublin_boundary": "https://codema-dev.s3.eu-west-1.amazonaws.com/dublin_admin_county_boundaries.zip",
    "dublin_small_area_boundaries": "https://codema-dev.s3.eu-west-1.amazonaws.com/dublin_small_area_boundaries_in_routing_keys.gpkg",
}

INPUT_FILEPATHS = {
    "dublin_boundary": DATA_DIR / "external" / "dublin_admin_county_boundaries.parquet",
    "roads": DATA_DIR / "external" / f"osm_roads_{DAY}_{MONTH}_{YEAR}.parquet",
    "dublin_small_area_boundaries": DATA_DIR
    / "external"
    / "dublin_small_area_boundaries_in_routing_keys.parquet",
}

OUTPUT_FILEPATHS = {
    "roads": DATA_DIR / "processed" / f"osm_roads_{DAY}_{MONTH}_{YEAR}.gpkg",
}

with Flow("Measure Small Area Road Line lengths") as flow:
    create_folder_structure = tasks.create_folder_structure(DATA_DIR)
    dublin_boundary = tasks.load_dublin_boundary(
        url=URLS["dublin_boundary"],
        filepath=INPUT_FILEPATHS["dublin_boundary"],
        columns=["geometry"],
    ).set_upstream(create_folder_structure)
    dublin_small_area_boundaries = tasks.load_dublin_small_area_boundaries(
        url=URLS["dublin_small_area_boundaries"],
        filepath=INPUT_FILEPATHS["dublin_small_area_boundaries"],
    )
    dublin_polygon = tasks.dissolve_boundaries_to_polygon(dublin_boundary)
    osm_highway = tasks.load_roads(
        dublin_polygon, INPUT_FILEPATHS["roads"], columns=["highway", "geometry"]
    )
    osm_roads = tasks.extract_lines(osm_highway)
    osm_roads_in_small_areas = tasks.cut_lines_on_boundaries(
        lines=osm_roads, boundaries=dublin_small_area_boundaries
    )

    tasks.save_to_gpkg(osm_roads_in_small_areas, OUTPUT_FILEPATHS["roads"])

state = flow.run()
flow.visualize(flow_state=state, filename=HERE / "flow", format="png")
