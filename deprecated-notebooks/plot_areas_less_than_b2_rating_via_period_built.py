# %%
from pathlib import Path

import dask.dataframe as dd
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

sns.set()
data_dir = Path("../data")

# %% [markdown]
# # Get 2016 Dublin Small Area Boundaries
dublin_small_area_boundaries = gpd.read_file(
    data_dir / "dublin_small_area_boundaries.gpkg",
    driver="GPKG",
).loc[:, ["SMALL_AREA", "geometry"]]

# %% [markdown]
# # Get 2016 Small Area Statistics

# %%
column_names = {
    "T6_2_PRE19H": "before 1919",
    "T6_2_19_45H": "1919 - 1945",
    "T6_2_46_60H": "1946 - 1960",
    "T6_2_61_70H": "1961 - 1970",
    "T6_2_71_80H": "1971 - 1980",
    "T6_2_81_90H": "1981 - 1990",
    "T6_2_91_00H": "1991 - 2000",
    "T6_2_01_10H": "2001 - 2010",
    "T6_2_11LH": "2011 or later",
    "T6_2_NSH": "not stated",
    "T6_2_TH": "total",
}
columns = ["SMALL_AREA"] + list(column_names.values())
census_2016_amalgamated = (
    pd.read_csv(data_dir / "SAPS2016_SA2017.csv")
    .rename(columns=column_names)
    .assign(SMALL_AREA=lambda df: df["GEOGID"].str[7:])
    .loc[:, columns]
    .melt(id_vars="SMALL_AREA", var_name="period_built")
    .merge(dublin_small_area_boundaries["SMALL_AREA"])
    .assign(value=lambda df: df["value"].astype(np.int32))
)

# %%
census_2016 = (
    census_2016_amalgamated.reindex(
        census_2016_amalgamated.index.repeat(census_2016_amalgamated["value"])
    )  # Expand census to one building per row
    .drop(columns="value")
    .query("period_built != ['not stated', 'total']")
    .reset_index(drop=True)
    .assign(
        age_bin=lambda df: df["period_built"].replace(
            {
                "before 1919": "before 2010",
                "1919 - 1945": "before 2010",
                "1946 - 1960": "before 2010",
                "1961 - 1970": "before 2010",
                "1971 - 1980": "before 2010",
                "1981 - 1990": "before 2010",
                "1991 - 2000": "before 2010",
                "2001 - 2010": "before 2010",
            }
        ),
        age_bin_count=lambda df: df.groupby(["SMALL_AREA", "age_bin"]).transform(
            "count"
        ),
    )
)


# %% [markdown]
# # Split the stock into < 2011 and > 2011
# ... as of 25/03/2021 this is a good guess at < B2 rating!
# 2000 - 2010 is the only age band that has a meaningful number >= B2 but nonetheless
# in the tail end...
age_bin_count = (
    census_2016[["SMALL_AREA", "age_bin", "age_bin_count"]]
    .pivot_table(values="age_bin_count", index="SMALL_AREA", columns="age_bin")
    .fillna(0)
    .reset_index()
    .assign(
        percentage_greater_than_b2=lambda df: df.eval(
            "(`2011 or later` / (`before 2010` + `2011 or later`)) * 100"
        ).round()
    )
)

# %%
small_area_boundaries_greater_than_b2 = dublin_small_area_boundaries.merge(
    age_bin_count
)

# %% [markdown]
# # Plot

# %%
f, ax = plt.subplots(figsize=(20, 20))
small_area_boundaries_greater_than_b2.plot(
    column="percentage_greater_than_b2",
    cmap="Greens",
    legend=True,
    ax=ax,
)
plt.title("% of Dwellings with a BER Rating Greater than B2", fontsize=20)

# %%
f.savefig(data_dir / "% of Dwellings with a BER Rating Greater than B2.png")

# %% [markdown]
# # Save
small_area_boundaries_greater_than_b2.to_file(
    data_dir / "small_area_boundaries_greater_than_b2.geojson", driver="GeoJSON"
)
