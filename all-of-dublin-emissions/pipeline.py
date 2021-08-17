from dotenv import load_dotenv

load_dotenv(".prefect")  # load local prefect configuration prior to import!
from prefect import Flow

import tasks
from globals import HERE
from globals import DATA_DIR

DIRPATHS = {"nta": DATA_DIR / "external" / ""}

with Flow("Extract infrastructure small area line lengths") as flow:
    create_folder_structure = tasks.create_folder_structure(DATA_DIR)

    check_nta_data_exists = tasks.check_file_exists(
        DIRPATHS["nta"], upstream_tasks=[create_folder_structure]
    )
    check_building_emissions_data_exists = tasks.check_file_exists(
        DIRPATHS["building_emissions"],
    )

state = flow.run()

flow.visualize(flow_state=state, filename=HERE / "flow", format="png")
