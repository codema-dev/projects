# %%
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

data_dir = Path("../data")

# %% [markdown]
# # Amalgamate Cross-tabulated Small Area building stock for all Dublin LAs

# %%
dublin_buildings_at_small_area = (
    pd.concat(
        [
            pd.read_csv(
                data_dir
                / "Census-2011-crosstabulations"
                / f"{local_authority}_SA_2011.csv"
            )
            for local_authority in ["DCC", "DLR", "FCC", "SD"]
        ]
    )
    .query("`sa_2011` != ['Dublin City', 'South Dublin']")
    .query("`period_built_unstandardised` != ['Total', 'All Houses']")
    .replace({">3": 1, "<3": 1, ".": np.nan})
    .dropna(subset=["value"])
    .assign(
        value=lambda df: df["value"].astype(np.int32),
        sa_2011=lambda df: df["sa_2011"].str.replace(r"_", r"/"),
        period_built_unstandardised=lambda df: df["period_built_unstandardised"]
        .str.lower()
        .str.replace("2006 or later", "2006 - 2011"),
    )
)

# %% [markdown]
# # Link 2011 Small Areas to Postcodes

# %%
dublin_small_area_boundaries["CountyName"] = (
    gpd.sjoin(
        dublin_small_area_boundaries.assign(
            geometry=lambda gdf: gdf.geometry.representative_point()
        ),
        ireland_postcode_boundaries,
        op="within",
        how="left",
    )
    .loc[:, "Descriptor"]
    .str.title()
    .replace(r"(^(?!Dublin|Bray|Maynooth).*$)", "Co. Dublin", regex=True)
)

# %% [markdown]
# # Expand each Small Area to Individual Buildings

# %%
def expand_to_indiv_buildings(stock, on="value"):
    return pd.DataFrame(stock.values.repeat(stock[on], axis=0), columns=stock.columns)


dublin_indiv_buildings_at_small_area = expand_to_indiv_buildings(
    dublin_buildings_at_small_area
).drop(columns="value")

# %% [markdown]
# # Anonymise stock to Postcode level

# %%
dublin_indiv_buildings_at_postcode_level = dublin_indiv_buildings_at_small_area.merge(
    dublin_small_area_boundaries, how="left", left_on="sa_2011", right_on="SMALL_AREA"
).loc[:, ["dwelling_type_unstandardised", "period_built_unstandardised", "CountyName"]]

# %% [markdown]
# # Save

# %%
dublin_indiv_buildings_at_postcode_level.to_csv(
    "../data/dublin_building_stock_up_to_2011.csv", index=False
)

# %%
