from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

data_dir = Path("../data")


def repeat_rows_on_column(df, on):
    return df.reindex(df.index.repeat(df[on])).drop(columns=on)


dublin_small_area_boundaries_2011 = gpd.read_file(
    data_dir / "dublin_small_area_boundaries_2011.geojson"
)

filepaths = [
    pd.read_csv(
        data_dir / "Census-2011-crosstabulations" / f"{local_authority}_SA_2011.csv"
    )
    for local_authority in ["DCC", "DLR", "FCC", "SD"]
]

census_2011_small_area_hhs = (
    pd.concat(filepaths)
    .query("`sa_2011` != ['Dublin City', 'South Dublin']")
    .query("`period_built_unstandardised` != ['Total', 'All Houses']")
    .replace({">3": 1, "<3": 1, ".": np.nan})
    .dropna(subset=["value"])
    .rename(
        columns={
            "sa_2011": "SMALL_AREA",
            "period_built_unstandardised": "period_built",
        }
    )
    .assign(
        value=lambda df: df["value"].astype(np.int32),
        SMALL_AREA=lambda df: df["SMALL_AREA"].str.replace(r"_", r"/"),
        period_built=lambda df: df["period_built"]
        .str.lower()
        .str.replace("2006 or later", "2006 - 2010"),
        dwelling_type=lambda df: df["dwelling_type_unstandardised"].replace(
            {
                "Flat/apartment in a purpose-built block": "Apartment",
                "Flat/apartment in a converted house or commercial building": "Apartment",
                "Bed-sit": "Apartment",
            }
        ),
    )
    .merge(
        dublin_small_area_boundaries_2011[
            ["SMALL_AREA", "EDNAME", "distance_to_city_centre_in_km"]
        ],
        how="outer",
    )
    .reset_index(drop=True)
    .drop(columns=["dwelling_type_unstandardised"])
)

census_2011_indiv_hhs = (
    repeat_rows_on_column(census_2011_small_area_hhs, "value")
    .query("period_built != ['total']")
    .assign(
        category_id=lambda df: df.groupby(
            ["SMALL_AREA", "dwelling_type", "period_built"]
        )
        .cumcount()
        .apply(lambda x: x + 1),
    )
)

census_2011_indiv_hhs.to_parquet(data_dir / "census_2011_indiv_hhs.parquet")