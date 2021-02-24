# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.10.2
#   kernelspec:
#     display_name: 'Python 3.9.2 64-bit (''dbs'': conda)'
#     metadata:
#       interpreter:
#         hash: 91b4ddbfb3ef6e4cea71efeb7c8e9fed5f228fde8c807fc357f7d211c22b6ea4
#     name: python3
# ---

# %%
from os import path
from shutil import unpack_archive
from urllib.request import urlretrieve

from berpublicsearch.download import download_berpublicsearch_parquet
import pandas as pd

# %%
path_to_census_stock = "../data/dublin_building_stock_up_to_2011.csv"
if not path.exists(path_to_census_stock):
    urlretrieve(
        url="https://zenodo.org/record/4552498/files/dublin_building_stock_up_to_2011.csv",
        filename=path_to_census_stock,
    )
residential_stock_pre_2011 = pd.read_csv(path_to_census_stock)

# %%
total_pre_2011 = (
    residential_stock_pre_2011.groupby("postcodes")["dwelling_type_unstandardised"]
    .count()
    .rename("pre_2011")
)

# %%
path_to_berpublicsearch = "../data/BERPublicsearch_parquet"
if not path.exists(path_to_berpublicsearch):
    download_berpublicsearch_parquet(
        email_address="rowan.molony@codema.ie", savedir="../data"
    )

berpublicsearch_ireland = dd.read_parquet(path_to_berpublicsearch)
berpublicsearch_dublin = (
    berpublicsearch_ireland[
        berpublicsearch_ireland["CountyName"].str.contains("Dublin")
    ]
    .query("`CountyName` != ['Dublin 23']")  # doesn't exist
    .compute()
)

# %%
for year in range(2016, 2021, 1):
    total = total_pre_2011 + (
        berpublicsearch_dublin.query(
            f"`Year_of_Construction` > 2011 and `Year_of_Construction` <= {year}"
        )
        .groupby("CountyName")["DwellingTypeDescr"]
        .count()
        .rename(f"post_{year}")
    )
    total.to_csv(f"../data/residential_total_buildings_{year}.csv")
