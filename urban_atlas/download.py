from pathlib import Path
from typing import Any

import fs
from fs.tools import copy_file_data


def fetch_s3_file(product: str, bucket: str) -> None:
    filepath = Path(product)
    filename = filepath.name
    if not filepath.exists():
        s3fs = fs.open_fs(bucket)
        with s3fs.open(filename, "rb") as remote_file:
            with open(filepath, "wb") as local_file:
                copy_file_data(remote_file, local_file)


def check_file_exists(product: str, filepath: str):
    assert Path(filepath).exists(), f"Please upload {Path(filepath).name} to data/raw"
