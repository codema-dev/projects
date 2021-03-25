# %%
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

sns.set()
data_dir = Path("../data")

# %%
ber_public = dd.read_parquet(data_dir / "BERPublicsearch_parquet")

# %%
keep_columns = [
    "CountyName",
    "Year_of_Construction",
    "EnergyRating",
]
ber_dublin = (
    ber_public.loc[:, keep_columns]
    .loc[ber_public["CountyName"].str.contains("Dublin")]
    .compute()
    .assign(
        cso_period_built=lambda df: pd.cut(
            df["Year_of_Construction"],
            bins=[-np.inf, 1919, 1945, 1960, 1970, 1980, 1990, 2000, 2010, np.inf],
            labels=[
                "before 1919",
                "1919 - 1945",
                "1946 - 1960",
                "1961 - 1970",
                "1971 - 1980",
                "1981 - 1990",
                "1991 - 2000",
                "2001 - 2010",
                "2011 or later",
            ],
        ),
        cso_period_built_count=lambda df: df.groupby("EnergyRating")["cso_period_built"]
        .value_counts()
        .reset_index(drop=True),
        age_bin=lambda df: pd.cut(
            df["Year_of_Construction"],
            bins=[-np.inf, 2000, 2010, np.inf],
            labels=[
                "before 2000",
                "2001 - 2010",
                "2011 or later",
            ],
        ),
    )
)

# %%
ber_dublin.pivot_table(
    index="EnergyRating",
    columns="cso_period_built",
    values="Year_of_Construction",
    aggfunc="count",
).plot.bar(rot=0, stacked=True, figsize=(15, 5))

# %%
ber_dublin.pivot_table(
    index="EnergyRating",
    columns="age_bin",
    values="age_bin_count",
    aggfunc="sum",
).plot.bar(rot=0, stacked=True, figsize=(15, 5))

# %%
