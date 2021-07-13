from pathlib import Path
import re
from urllib.request import urlretrieve

import numpy as np
import pandas as pd


def download(url: str, filename: str) -> None:
    if not Path(filename).exists():
        urlretrieve(url=url, filename=filename)


def _repeat_rows_on_column(df, on):
    return df.reindex(df.index.repeat(df[on])).drop(columns=on)


def extract_period_built_statistics(statistics: pd.DataFrame) -> pd.DataFrame:
    columns_to_extract = [
        x for x in statistics.columns if re.match(r"T6_2_.*H", x) or x == "GEOGID"
    ]
    return statistics.copy().loc[:, columns_to_extract]


def melt_small_area_statistics_to_individual_buildings(
    statistics: pd.DataFrame,
) -> pd.DataFrame:
    """Wrangle the stock to individual building level.

    Before:
        GEOGID              T6_2_PRE19H     ...
        SA2017_017001001    19              ...

    After:
        small_area          period_built
        017001001           PRE19H
        017001001           PRE19H

    Args:
        sa_stats_raw (pd.DataFrame): overview of buildings

    Returns:
        pd.DataFrame: individual buildings
    """
    return (
        statistics.copy()
        .assign(small_area=lambda df: df["GEOGID"].str[7:])
        .drop(columns="GEOGID")
        .set_index("small_area")
        .rename(columns=lambda x: re.findall(f"T6_2_(.*)H", x)[0])
        .reset_index()
        .melt(id_vars="small_area", var_name="period_built", value_name="total")
        .query("period_built != 'T'")
        .pipe(_repeat_rows_on_column, on="total")
    )


def replace_not_stated_period_built_with_mode(stock: pd.DataFrame) -> pd.Series:
    modal_period_built = (
        stock.assign(period_built=lambda df: df["period_built"].replace({"NS": np.nan}))
        .groupby("small_area")["period_built"]
        .transform(lambda s: s.mode()[0])
    )
    return stock.assign(
        period_built=lambda df: df["period_built"]
        .replace({"NS": np.nan})
        .fillna(modal_period_built)
    )


def link_small_areas_to_countyname(
    stock: pd.DataFrame, routing_keys: pd.DataFrame
) -> pd.DataFrame:
    breakpoint()


def to_parquet(df: pd.DataFrame, path: str) -> None:
    df.to_parquet(path)
