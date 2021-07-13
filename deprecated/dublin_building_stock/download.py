from pathlib import Path
from shutil import unpack_archive
from typing import Union
import requests

from loguru import logger
from tqdm import tqdm
from valuation_office_ireland.download import download_valuation_office_categories
from berpublicsearch.download import download_berpublicsearch_parquet

FilePath = Union[Path, str]
logger.remove()
logger.add(lambda msg: tqdm.write(msg, end=""))


def _download(url, filepath):

    response = requests.get(url)
    try:
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Request failed with {e}")

    chunk_size = 1024  # i.e. 1 byte
    with tqdm.wrapattr(
        open(str(filepath), "wb"),
        "write",
        miniters=1,
        desc=str(filepath),
        total=getattr(response, "length", None),
    ) as fout:
        for chunk in response.iter_content(chunk_size=chunk_size):
            fout.write(chunk)


def _unzip(input_filepath, output_filepath):
    return unpack_archive(
        input_filepath,
        output_filepath,
    )


def download(url: str, filepath: FilePath, latest: bool = False) -> None:
    filepath = Path(filepath)
    if filepath.exists() and latest is False:
        logger.info(
            f"Skipping download as {filepath} already exists "
            f" & download latest version is {latest}"
        )
    else:
        logger.info(f"Downloading {filepath.stem} to {filepath} via {url}...")
        _download(url, filepath)
        if "zip" in str(filepath):
            logger.info(f"Unzipping {filepath.stem} to {filepath.with_suffix('').stem}")
            _unzip(filepath, filepath.with_suffix(""))


def download_dublin_valuation_office(data_dir: FilePath, latest: bool = False):
    vo_data_dir = Path(data_dir) / "valuation_office"
    local_authorities = [
        "DUN LAOGHAIRE RATHDOWN CO CO",
        "DUBLIN CITY COUNCIL",
        "FINGAL COUNTY COUNCIL",
        "SOUTH DUBLIN COUNTY COUNCIL",
    ]
    if vo_data_dir.exists() and latest is False:
        logger.info(
            f"Skipping download as {vo_data_dir} already exists "
            f" & download latest version is {latest}"
        )
    else:
        download_valuation_office_categories(
            savedir=vo_data_dir,
            local_authorities=local_authorities,
        )


def download_ber_public(data_dir: FilePath, email_address: str, latest: bool = False):
    filepath = data_dir / "BERPublicsearch_parquet"
    if filepath.exists() and latest is False:
        logger.info(
            f"Skipping download as {filepath} already exists "
            f" & download latest version is {latest}"
        )
    else:
        download_berpublicsearch_parquet(
            email_address=email_address,
            savedir=data_dir,
        )