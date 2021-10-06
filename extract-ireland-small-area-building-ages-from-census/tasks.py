import re
from typing import Any

import pandas as pd


def extract_period_built_statistics(product: Any, upstream: Any) -> None:
    statistics = pd.read_csv(upstream["download_census_small_area_statistics_2016"])
    building_age_columns = [x for x in statistics.columns if re.match(r"T6_2_.*H", x)]
    building_age_statistics = statistics[building_age_columns].rename(
        columns=lambda x: re.findall(f"T6_2_(.*)H", x)[0]
    )
    small_areas = statistics["GEOGID"].str[7:].rename("small_area")
    select_statistics = pd.concat([small_areas, building_age_statistics], axis=1)
    select_statistics.to_csv(product, index=False)


def _repeat_rows_on_column(df, on):
    return df.reindex(df.index.repeat(df[on])).drop(columns=on)


def melt_small_area_period_built_to_individual_buildings(
    product: Any, upstream: Any,
) -> None:
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
    small_area_period_built = pd.read_csv(upstream["extract_period_built_statistics"])
    individual_building_period_built =  (
        small_area_period_built.melt(id_vars="small_area", var_name="period_built", value_name="total")
        .query("period_built != 'T'")
        .pipe(_repeat_rows_on_column, on="total")
    )
    individual_building_period_built.to_csv(product, index=False)

