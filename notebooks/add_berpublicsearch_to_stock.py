# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.10.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
from os import path
from shutil import unpack_archive
from urllib.request import urlretrieve

from berpublicsearch.download import download_berpublicsearch_parquet
import dask.dataframe as dd
import pandas as pd

# %% [markdown]
# # Get 2011 Stock

# %%
path_to_census_stock = "../data/dublin_building_stock_up_to_2011.csv"
if not path.exists(path_to_census_stock):
    urlretrieve(
        url="https://zenodo.org/record/4552498/files/dublin_building_stock_up_to_2011.csv",
        filename=path_to_census_stock,
    )

dublin_residential_stock_pre_2011 = pd.read_csv(path_to_census_stock)

# %% [markdown]
# # Get BER Public Search Database

# %%
path_to_berpublicsearch = "../data/BERPublicsearch_parquet"
if not path.exists(path_to_berpublicsearch):
    download_berpublicsearch_parquet(
        email_address="rowan.molony@codema.ie", savedir="../data"
    )

berpublicsearch = dd.read_parquet(path_to_berpublicsearch)
berpublicsearch_dublin = berpublicsearch[
    berpublicsearch["CountyName"].str.contains("Dublin")
].compute()

# %% [markdown]
# # Standardise <2011 & >2011 Stocks so can merge on common columns

# %%
dublin_residential_stock_pre_2011_standardised = (
    dublin_residential_stock_pre_2011.assign(
        dwelling_type=lambda df: df["dwelling_type_unstandardised"].replace(
            {
                "Flat/apartment in a purpose-built block": "Apartment",
                "Flat/apartment in a converted house or commercial building": "Apartment",
                "Bed-sit": "Apartment",
            }
        )
    )
    .loc[:, ["postcodes", "dwelling_type", "regulation_period_deap_appendix_s"]]
    .copy()
)

# %%
dublin_residential_stock_post_2011_standardised = (
    berpublicsearch_dublin.query("`Year_of_Construction` > 2011")
    .assign(
        regulation_period_deap_appendix_s=lambda df: ">2010",
        dwelling_type=lambda df: df["DwellingTypeDescr"].replace(
            {
                "Mid-floor apartment": "Apartment",
                "Top-floor apartment": "Apartment",
                "Ground-floor apartment": "Apartment",
                "Maisonette": "Apartment",
                "Basement Dwelling": "Apartment",
                "Mid-terrace house": "Terraced house",
                "End of terrace house": "Terraced house",
            }
        ),
    )
    .query("`dwelling_type` != 'House'")
    .rename(columns={"CountyName": "postcodes"})
    .loc[:, ["postcodes", "dwelling_type", "regulation_period_deap_appendix_s"]]
    .copy()
)

# %% [markdown]
# # Add post 2011 BER buildings to 2011 Stock

# %%
dublin_residential_stock = pd.concat(
    [
        dublin_residential_stock_pre_2011_standardised,
        dublin_residential_stock_post_2011_standardised,
    ]
).reset_index(drop=True)

# %%
dublin_residential_stock.to_csv(
    "../data/dublin_residential_stock_23_02_2021.csv", index=False
)

# %%
dublin_residential_stock

# %% [markdown]
#
