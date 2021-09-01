import dotenv
import fsspec
from prefect import Flow
from prefect import task

import tasks

dotenv.load_dotenv()

FILEPATHS = {
    "bers": tasks.get_data("data/external/small_area_bers.parquet"),
    "data": tasks.get_data("data"),
    "external": tasks.get_data("data/external"),
}

check_s3_credentials_are_defined = task(
    tasks.check_if_s3_keys_are_defined, name="Check S3 Credentials are defined"
)
create_data_folders = task(tasks.create_folder_structure, name="Create Data folders")
download_bers = task(tasks.fetch_s3_file, name="Download Small Area BERs")

estimate_retrofit_costs = task(tasks.execute_python_file, name="Execute Python File")

with Flow("Retrofit all Dublin Dwellings to HLP=2") as flow:
    check_s3_credentials_are_defined_task = check_s3_credentials_are_defined()
    create_data_folders_task = create_data_folders(FILEPATHS["data"])
    download_bers_task = download_bers(
        bucket="s3://codema-dev",
        filename="small_area_bers.parquet",
        savedir=FILEPATHS["external"],
    )

    estimate_retrofit_costs_task = estimate_retrofit_costs(
        py_filepath="estimate_retrofit_costs.py",
        ipynb_filepath="estimate_retrofit_costs.ipynb",
        parameters={
            "DATA_DIR": str(FILEPATHS["data"]),
            "ber_filepath": str(FILEPATHS["bers"]),
        },
    )

    # set dependencies
    create_data_folders_task.set_upstream(check_s3_credentials_are_defined_task)
    download_bers_task.set_upstream(create_data_folders_task)


state = flow.run()
