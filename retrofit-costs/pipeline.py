import dotenv
import fsspec
from prefect import Flow
from prefect import task

import tasks

dotenv.load_dotenv()

FILEPATHS = {"external": tasks.get_data("data/external")}

check_s3_credentials_are_defined = task(
    tasks.check_if_s3_keys_are_defined, name="Check S3 Credentials are defined"
)
create_data_folders = task(tasks.create_folder_structure, name="Create Data folders")
load_bers = task(fsspec.open, name="Load Small Area BERs")


with Flow("Retrofit all Dublin Dwellings to HLP=2") as flow:
    check_s3_credentials_are_defined_task = check_s3_credentials_are_defined()
    create_data_folders_task = create_data_folders(tasks.get_data("data"))
    bers_file = load_bers(
        url="filecache::s3://codema-dev/small_area_bers.parquet",
        filecache={"cache_storage": FILEPATHS["external"]},
    )

    # set dependencies
    create_data_folders_task.set_upstream(check_s3_credentials_are_defined_task)
    bers_file.set_upstream(create_data_folders_task)


state = flow.run()
