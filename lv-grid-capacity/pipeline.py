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
    substations = tasks.load_esb_substation_data(URLS["grid"]).set_upstream(
        create_folder_structure
    )

state = flow.run()
flow.visualize(flow_state=state, filename=HERE / "flow", format="png")
