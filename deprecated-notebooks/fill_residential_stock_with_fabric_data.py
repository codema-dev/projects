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
import dask.dataframe as dd
import numpy as np
import pandas as pd

# %% [markdown]
# # Run `add_berpublicsearch_to_stock.ipynb` to generate `data/dublin_residential_stock_23_02_2021.csv`

# %% [markdown]
# # Get Residential Stock & standardise it

# %%
residential_stock = pd.read_csv("../data/dublin_residential_stock_23_02_2021.csv")

# %%
residential_stock_archetype_totals = (
    residential_stock.groupby(["postcodes", "dwelling_type", "regulation_period_deap_appendix_s"])
    ["regulation_period_deap_appendix_s"].count()
    .rename("archetype_totals")
    .reset_index()
    .query("`archetype_totals` > 30")
)

# %%
residential_stock_standardised = residential_stock.assign(
    archetype_id=(
        lambda df: df.groupby(["postcodes", "dwelling_type", "regulation_period_deap_appendix_s"])
        .cumcount()
        .apply(lambda x: x + 1)  # add 1 to each row as cumcount starts at 0
    )
)

# %%
residential_stock_standardised

# %% [markdown]
# # Get BER Public search & standardise it

# %%
berpublicsearch = dd.read_parquet("../data/BERPublicsearch_parquet")
berpublicsearch_dublin = berpublicsearch[berpublicsearch["CountyName"].str.contains("Dublin")].compute()

# %%
berpublicsearch_dublin_standardised = (
    berpublicsearch_dublin.assign(
        regulation_period_deap_appendix_s=lambda df:pd.cut(
            df["Year_of_Construction"],
            bins=[-np.inf, 1978, 1983, 1994, 2000, 2005, 2010, np.inf],
        ).map(
            {
                pd.Interval(-np.inf, 1978): "<1978",
                pd.Interval(1978, 1983): "1983 - 1993",
                pd.Interval(1983, 1994): "1983 - 1993",
                pd.Interval(1994, 2000): "1994 - 1999",
                pd.Interval(2000, 2005): "2000 - 2004",
                pd.Interval(2005, 2010): "2005 - 2009",
                pd.Interval(2010, np.inf): ">2010",
            }
        ),
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
        archetype_id=(
            lambda df: df.groupby(["CountyName", "dwelling_type", "regulation_period_deap_appendix_s"])
            .cumcount()
            .apply(lambda x: x + 1)  # add 1 to each row as cumcount starts at 0
        ),
    )
    .query("`dwelling_type` != 'House'")
    .query("`CountyName` != ['Dublin 21', 'Dublin 19', 'Dublin 23']") # they don't exist
    .rename(columns={"CountyName":"postcodes"})
)

# %%
demand_column_names = [
    'DeliveredEnergyMainSpace',
    'DeliveredEnergyMainWater',
    'DeliveredEnergySecondarySpace',
]
boiler_efficiency_column_names = [
    'HSMainSystemEfficiency',
    'WHMainSystemEff',
    'HSSupplSystemEff',
]

berpublicsearch_dublin_standardised["total_heat_demand"] = sum([
    np.abs(berpublicsearch_dublin_standardised[demand_col] * berpublicsearch_dublin_standardised[efficiency_col] / 100) # eliminate -ve demand values
    for demand_col, efficiency_col in zip(demand_column_names, boiler_efficiency_column_names)
])

# %% [markdown]
# # Fill residential stock with corresponding fabric data

# %%
merge_columns = ["postcodes", "dwelling_type", "regulation_period_deap_appendix_s", "archetype_id"]
residential_stock_with_ber_and_demand = pd.merge(
    residential_stock_standardised,
    berpublicsearch_dublin_standardised[merge_columns + ["EnergyRating", "total_heat_demand"]],
    on=merge_columns,
    how="left"
)

# %%
residential_stock_with_ber_and_demand.to_csv("../data/residential_stock_with_ber_and_heat_demand.csv", index=False)
