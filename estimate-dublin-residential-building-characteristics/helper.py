from csv import QUOTE_NONE
from zipfile import ZipFile

import pandas as pd


def read_bers(filepath, **kwargs):
    with ZipFile(filepath, "r") as zf:
        with zf.open("BERPublicsearch.txt", "r") as f:
            return pd.read_csv(
                f, sep="\t", encoding="latin-1", quoting=QUOTE_NONE, **kwargs
            )
