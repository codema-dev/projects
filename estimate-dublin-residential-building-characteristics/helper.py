from csv import QUOTE_NONE
from pathlib import Path
from shutil import unpack_archive
from zipfile import ZipFile

import dask.dataframe as dd
import pandas as pd


def _read_bers_via_dask(filepath, **kwargs):
    unzipped_filepath = Path(filepath).with_suffix("")
    if not unzipped_filepath.exists():
        unpack_archive(filepath, unzipped_filepath)
    return dd.read_csv(
        unzipped_filepath / "BERPublicsearch.txt",
        sep="\t",
        encoding="latin-1",
        quoting=QUOTE_NONE,
        **kwargs
    )


def _read_bers_via_pandas(filepath, **kwargs):
    with ZipFile(filepath, "r") as zf:
        with zf.open("BERPublicsearch.txt", "r") as f:
            return pd.read_csv(
                f, sep="\t", encoding="latin-1", quoting=QUOTE_NONE, **kwargs
            )


def read_bers(filepath, how="pandas", **kwargs):
    _read_map = {
        "pandas": _read_bers_via_pandas,
        "dask": _read_bers_via_dask,
    }
    _read = _read_map[how]
    return _read(filepath, **kwargs)
