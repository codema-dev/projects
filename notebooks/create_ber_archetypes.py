import json
from pathlib import Path

import pandas as pd
import numpy as np

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


def create_archetypes(stock, column_operations, archetype_columns):
    sample_size = stock.groupby(archetype_columns).size().rename("sample_size")
    use_columns = archetype_columns + list(column_operations.keys())
    return (
        stock.loc[:, use_columns]
        .groupby(archetype_columns)
        .agg(column_operations)
        .join(sample_size)
        .query("sample_size > 30")
    )


dublin_ber_private = pd.read_parquet(data_dir / "dublin_ber_private.parquet")

use_columns = [
    "EDNAME",
    "SMALL_AREA_2016",
    "BERBand",
    "Energy Value",
    "dwelling_type",
    "period_built",
    "total_floor_area",
    "Ventilation Method Description",
    "Wall weighted Uvalue",
    "Door Weighted Uvalue",
    "Roof Weighted Uvalue",
    "Floor Weighted Uvalue",
    "WindowsWeighted Uvalue",
    "Wall Total Area",
    "Door Total Area",
    "Roof Total Area",
    "Floor Total Area",
    "Windows Total Area",
    "effective_air_rate_change",
    "No Of Storeys",
    "Most Significant Wall Type",
]
dublin_ber_private_extract = (
    dublin_ber_private.loc[:, use_columns]
    .assign(
        regulatory_period=lambda df: pd.cut(
            df["period_built"].cat.codes,
            bins=[-np.inf, 3, 4, 5, 6, 7, 8, 9],
            labels=[
                "before 1970",
                "1971 - 1980",
                "1981 - 1990",
                "1991 - 2000",
                "2001 - 2005",
                "2006 - 2010",
                "2011 or later",
            ],
        ),
        most_significant_wall_type=lambda df: df["Most Significant Wall Type"].replace(
            {"Other": np.nan}
        ),
    )
    .query("dwelling_type != 'Not stated'")
)

area_columns = [
    c
    for c in use_columns
    if "area" in c.lower() and not any(x in c.lower() for x in ["roof", "door"])
]
# remove all zero floor, wall, window areas
dublin_ber_private_extract.loc[:, area_columns] = dublin_ber_private_extract[
    area_columns
].replace({0: np.nan})


column_operations = {
    "Energy Value": "median",
    "total_floor_area": "median",
    "Ventilation Method Description": pd.Series.mode,
    "Wall weighted Uvalue": "median",
    "Door Weighted Uvalue": "median",
    "Roof Weighted Uvalue": "median",
    "Floor Weighted Uvalue": "median",
    "WindowsWeighted Uvalue": "median",
    "Wall Total Area": "median",
    "Door Total Area": "median",
    "Roof Total Area": "median",
    "Floor Total Area": "median",
    "Windows Total Area": "median",
    "effective_air_rate_change": pd.Series.mode,
    "No Of Storeys": "median",
    "most_significant_wall_type": get_most_common_wall_type,
}

archetypes_small_area = create_archetypes(
    dublin_ber_private_extract,
    column_operations=column_operations,
    archetype_columns=["SMALL_AREA_2016", "dwelling_type", "period_built"],
)
archetypes_small_area.to_parquet(data_dir / "archetypes_small_area.parquet")

archetypes_electoral_district = create_archetypes(
    dublin_ber_private_extract,
    column_operations=column_operations,
    archetype_columns=["EDNAME", "dwelling_type", "period_built"],
)
archetypes_electoral_district.to_parquet(
    data_dir / "archetypes_electoral_district.parquet"
)

archetypes_all_of_dublin = create_archetypes(
    dublin_ber_private_extract,
    column_operations=column_operations,
    archetype_columns=["dwelling_type", "period_built"],
)
archetypes_all_of_dublin.to_parquet(data_dir / "archetypes_all_of_dublin.parquet")

archetypes_dwelling_type = create_archetypes(
    dublin_ber_private_extract,
    column_operations=column_operations,
    archetype_columns=["dwelling_type"],
)
archetypes_dwelling_type.to_parquet(data_dir / "archetypes_dwelling_type.parquet")

archetypes_period_built = create_archetypes(
    dublin_ber_private_extract,
    column_operations=column_operations,
    archetype_columns=["period_built"],
)
archetypes_period_built.to_parquet(data_dir / "archetypes_period_built.parquet")

archetype_new_build = (
    create_archetypes(
        dublin_ber_private_extract,
        column_operations=column_operations,
        archetype_columns=["period_built"],
    )
    .loc["2011 or later"]
    .to_dict()
)
archetype_new_build["regulatory_period"] = "2011 or later"
archetype_new_build_standardised = {
    k: v.item() if not isinstance(v, str) else v for k, v in archetype_new_build.items()
}  # convert numpy dtypes to standard Python so json serializable
with open(data_dir / "archetype_new_build.json", "w") as file:
    json.dump(archetype_new_build_standardised, file)