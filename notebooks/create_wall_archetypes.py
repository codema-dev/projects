from pathlib import Path

import numpy as np
import pandas as pd

data_dir = Path("../data")


def get_most_common_wall_type(s: pd.Series) -> pd.Series:
    mode_raw = pd.Series.mode(s)
    if len(mode_raw) > 1:
        # if multiple modes, take the 1st mode
        mode = mode_raw.iloc[0]
    elif mode_raw.empty:
        # if no mode found assume "300mm Cavity" which represents 65/260
        # (note: 2nd most common is "Concrete Hollow Block" which is 35/260)
        mode = "300mm Cavity"
    else:
        mode = mode_raw
    return mode


most_common_wall_type_archetypes_by_location = (
    pd.read_parquet(data_dir / "dublin_ber_public.parquet")
    .assign(
        wall_type=lambda df: df["FirstWallType_Description"].replace({"Other": np.nan}),
        city_centre=lambda df: np.where(
            df["distance_to_city_centre_in_km"] <= 3.5, True, False
        ),
    )
    .groupby(["city_centre", "dwelling_type", "period_built"])["wall_type"]
    .agg(get_most_common_wall_type)
    .rename("WallType")
)

most_common_wall_type_archetypes_by_location.to_csv(
    data_dir / "most_common_wall_type_archetypes_by_location.csv"
)

most_common_wall_type_archetypes = (
    pd.read_parquet(data_dir / "dublin_ber_public.parquet")
    .assign(
        wall_type=lambda df: df["FirstWallType_Description"].replace({"Other": np.nan})
    )
    .groupby(["dwelling_type", "period_built"])["wall_type"]
    .agg(get_most_common_wall_type)
    .rename("WallType")
)

most_common_wall_type_archetypes.to_csv(
    data_dir / "most_common_wall_type_archetypes.csv"
)

total_wall_type_by_archetype = (
    pd.read_parquet(data_dir / "dublin_ber_public.parquet")
    .assign(
        wall_type=lambda df: df["FirstWallType_Description"].replace({"Other": np.nan})
    )
    .groupby(["dwelling_type", "period_built", "wall_type"])
    .size()
    .unstack()
    .assign(total=lambda df: df.sum(axis=1))
)

probablistic_wall_type_archetypes = (
    total_wall_type_by_archetype.drop(columns="total")
    .divide(total_wall_type_by_archetype["total"], axis=0)
    .round(2)
)

probablistic_wall_type_archetypes.to_csv(
    data_dir / "probablistic_wall_type_archetypes.csv"
)
