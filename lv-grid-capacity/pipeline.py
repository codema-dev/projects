from dotenv import load_dotenv

load_dotenv(".prefect")
from prefect import Flow

import tasks
from globals import DATA_DIR
from globals import HERE

URLS = {
    "grid": "https://codema-dev.s3.eu-west-1.amazonaws.com/heatmap-download-version-nov-2020.csv"
}

with Flow("Estimate LV capacity") as flow:
    create_folder_structure = tasks.create_folder_structure(DATA_DIR)
    raw_substations = tasks.load_esb_substation_data(URLS["grid"]).set_upstream(
        create_folder_structure
    )
    substations = tasks.convert_to_geodataframe(
        raw_substations,
        x="Longitude",
        y="Latitude",
        from_crs="EPSG:4326",
        to_crs="EPSG:2157",
    )
    lv_substations = tasks.query(substations, "`Voltage Class` == 'LV'")


state = flow.run()
flow.visualize(flow_state=state, filename=HERE / "flow", format="png")
