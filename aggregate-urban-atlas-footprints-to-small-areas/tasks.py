from os import PathLike
from pathlib import Path

def check_file_exists(product: PathLike, filepath: str):
    assert Path(filepath).exists(), f"Please upload {Path(filepath).name} to data/raw"
