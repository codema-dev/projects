import os
from pathlib import Path

import fs
from fs.tools import copy_file_data


def get_data(filepath: str) -> Path:
    return Path(__name__).parent / filepath


def create_folder_structure(data_dirpath: Path) -> None:
    data_dirpath.mkdir(exist_ok=True)
    external_dir = data_dirpath / "external"
    external_dir.mkdir(exist_ok=True)
    interim_dir = data_dirpath / "interim"
    interim_dir.mkdir(exist_ok=True)
    processed_dir = data_dirpath / "processed"
    processed_dir.mkdir(exist_ok=True)


def check_if_s3_keys_are_defined() -> None:
    message = f"""

        Please create a .env file
        
        In this directory: {Path.cwd().resolve()}

        With the following contents:
        
        AWS_ACCESS_KEY_ID=YOUR_KEY
        AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
    """
    assert os.getenv("AWS_ACCESS_KEY_ID") is not None, message
    assert os.getenv("AWS_SECRET_ACCESS_KEY") is not None, message


def fetch_s3_file(bucket: str, filename: str, savedir: Path) -> None:
    savepath = savedir / filename
    if not savepath.exists():
        s3fs = fs.open_fs(bucket)
        with s3fs.open(filename, "rb") as remote_file:
            with open(savedir / filename, "wb") as local_file:
                copy_file_data(remote_file, local_file)
