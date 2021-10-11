from shutil import unpack_archive
from typing import Dict

from os import PathLike
from pathlib import Path


def check_file_exists(product: PathLike, filepath: PathLike):
    assert Path(filepath).exists(), f"Please upload {Path(filepath).name} to data/raw"


def unzip_nta_rail_links_data(product: PathLike, upstream: Dict[str, PathLike]) -> None:
    unpack_archive(
        filename=upstream["check_nta_rail_links_are_uploaded"],
        extract_dir=Path(product).parent,
    )


def unzip_nta_grid_boundaries_data(
    product: PathLike, upstream: Dict[str, PathLike]
) -> None:
    unpack_archive(
        filename=upstream["check_nta_grid_boundaries_are_uploaded"],
        extract_dir=Path(product).parent,
    )
