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
}

INPUT_FILEPATHS = {
    "dublin_boundary": DATA_DIR / "external" / "dublin_admin_county_boundaries.parquet",
    "roads": DATA_DIR / "external" / f"osm_roads_{day}_{month}_{year}.parquet",
}

with Flow("Measure Small Area Road Line lengths") as flow:
    create_folder_structure = tasks.create_folder_structure(DATA_DIR)
    dublin_boundary = tasks.load_dublin_boundary(
        url=URLS["dublin_boundary"],
        filepath=INPUT_FILEPATHS["dublin_boundary"],
        columns=["geometry"],
    ).set_upstream(create_folder_structure)
    dublin_polygon = tasks.dissolve_boundaries_to_polygon(dublin_boundary)
    osm_roads = tasks.load_roads(dublin_polygon)

state = flow.run()
flow.visualize(flow_state=state, filename=HERE / "flow", format="png")
