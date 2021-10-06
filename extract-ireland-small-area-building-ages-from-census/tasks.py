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


