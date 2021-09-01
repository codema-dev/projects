import os
from pathlib import Path

from fugue import FugueWorkflow
import fsspec
import pandas as pd


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


def open_file(url: str, filepath: Path) -> None:
    return fsspec.open("filecache::" + url, filecache={"cache_storage": filepath})
