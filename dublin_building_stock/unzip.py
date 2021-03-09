from pathlib import Path
from shutil import unpack_archive
from typing import Union

from loguru import logger

FilePath = Union[Path, str]


def unzip_file(filepath: FilePath, extract_dir: FilePath) -> None:
    filepath = Path(filepath)
    extract_dir = Path(extract_dir)
    savepath = extract_dir / filepath.name
    if savepath.exists():
        logger.info(f"Skipping unzip as {savepath} already exists...")
    else:
        unpack_archive(filepath, extract_dir)
