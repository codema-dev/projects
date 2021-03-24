# %%
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import numpy as np

data_dir = Path("../data")

# %%
# Dropping 'Not stated' rows reduces stock from 455,634 to  427,655
dublin_pre_2011 = (
    pd.read_csv(data_dir / "dublin_building_stock_up_to_2011.csv")
    .query(
        "dwelling_type_unstandardised != 'not stated'"
        "and period_built_unstandardised != 'not stated'"
    )
    .rename(columns={"period_built_unstandardised": "period_built"})
    .assign(
        dwelling_type=lambda df: df["dwelling_type_unstandardised"].replace(
            {
                "Flat/apartment in a purpose-built block": "Apartment",
                "Flat/apartment in a converted house or commercial building": "Apartment",
                "Bed-sit": "Apartment",
            }
        ),
        category_total=lambda df: df.groupby(
            ["CountyName", "dwelling_type", "period_built"]
        ).transform("count"),
        category_id=lambda df: df.groupby(
            ["CountyName", "dwelling_type", "period_built"]
        )
        .cumcount()
        .apply(lambda x: x + 1),  # add 1 to each row as cumcount starts at 0
    )
    .drop(columns="dwelling_type_unstandardised")
)

# %%
ber_public = dd.read_parquet(data_dir / "BERPublicsearch_parquet")

# %%
keep_columns = [
    "category_id",
    "CountyName",
    "dwelling_type",
    "period_built",
    "Year_of_Construction",
    "EnergyRating",
]
ber_dublin = (
    ber_public[ber_public["CountyName"].str.contains("Dublin")]
    .compute()
    .query("DwellingTypeDescr != ['House', 'Maisonette', 'Basement Dwelling']")
    .assign(
        dwelling_type=lambda df: df["DwellingTypeDescr"].replace(
            {
                "Mid-floor apartment": "Apartment",
                "Top-floor apartment": "Apartment",
                "Ground-floor apartment": "Apartment",
                "Mid-terrace house": "Terraced House",
                "End of terrace house": "Terraced House",
            }
        ),
        period_built=lambda df: pd.cut(
            df["Year_of_Construction"],
            bins=[
                -np.inf,
                1919,
                1945,
                1960,
                1970,
                1980,
                1990,
                2000,
                2005,
                2010,
                np.inf,
            ],
            labels=[
                "before 1919",
                "1919 - 1945",
                "1946 - 1960",
                "1961 - 1970",
                "1971 - 1980",
                "1981 - 1990",
                "1991 - 2000",
                "2001 - 2005",
                "2006 - 2011",
                "2011 or later",
            ],
        ),
        category_id=lambda df: df.groupby(
            ["CountyName", "dwelling_type", "period_built"]
        )
        .cumcount()
        .apply(lambda x: x + 1),  # add 1 to each row as cumcount starts at 0
    )
    .loc[:, keep_columns]
)

# %%
latest_stock_raw = dublin_pre_2011.merge(
    ber_dublin,
    on=["category_id", "CountyName", "dwelling_type", "period_built"],
    how="outer",
    indicator=True,
)  # should only keep post 2011 buildings not in dublin_pre_2011

# %%
mask = (latest_stock_raw["_merge"] == "right_only") & (
    latest_stock_raw["Year_of_Construction"] < 2011
)
latest_stock_clean = latest_stock_raw.loc[~mask]

# %% [markdown]
# # Save
latest_stock_raw.to_csv(data_dir / "residential_stock_24_03_2021.csv", index=False)

# %%
