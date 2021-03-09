from pathlib import Path
from typing import Union

from loguru import logger
from requests import get
from requests import Response
from tqdm import tqdm

FilePath = Union[Path, str]
logger.remove()
logger.add(lambda msg: tqdm.write(msg, end=""))


def _save_file(response: Response, filepath: FilePath) -> None:
    """Download file to filepath via a HTTP response.

    Args:
        response (Response): A HTTP response from a request
        filepath (str): Save path destination for downloaded file
    """
    total_size_in_bytes = int(response.headers.get("content-length", 0))
    block_size = 1024  # 1 Kilobyte
    progress_bar = tqdm(total=total_size_in_bytes, unit="iB", unit_scale=True)

    with open(filepath, "wb") as save_destination:

        for stream_data in response.iter_content(block_size):
            progress_bar.update(len(stream_data))
            save_destination.write(stream_data)

    progress_bar.close()


def download(url: str, filepath: FilePath, latest: bool = False) -> None:
    filepath = Path(filepath)
    if filepath.exists() and latest is False:
        logger.info(
            f"Skipping download as {filepath} already exists "
            f" & download latest version is {latest}"
        )
    else:
        logger.info(f"Downloading {filepath.stem} to {filepath} via {url}...")
        response = get(url=url, stream=True)
        _save_file(response, filepath)
