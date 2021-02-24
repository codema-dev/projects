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

import geopandas as gpd
import numpy as np
import pandas as pd

from ods import join

# %% [markdown]
# # Amalgamate Cross-tabulated Small Area building stock for all Dublin LAs

# %%
dublin_buildings_at_small_area = (
    pd.concat(
        [
            pd.read_csv(f"../data/{local_authority}_SA_2011.csv")
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
# # Get 2011 Small Area Boundaries

# %%
if not path.exists("../data/Census2011_Small_Areas_generalised20m.zip"):
    urlretrieve(
        url="http://census.cso.ie/censusasp/saps/boundaries/Census2011_Small_Areas_generalised20m.zip",
        filename="../data/Census2011_Small_Areas_generalised20m.zip",
    )
    unpack_archive(
        "../data/Census2011_Small_Areas_generalised20m.zip",
        "../data/Census2011_Small_Areas_generalised20m",
    )

small_area_boundaries = gpd.read_file("../data/Census2011_Small_Areas_generalised20m")[
    ["SMALL_AREA", "EDNAME", "geometry"]
].to_crs(epsg=2157)

# %% [markdown]
# # Get Postcode boundaries

# %%
if not path.exists("../data/dublin_postcode_boundaries.zip"):
    urlretrieve(
        url="https://zenodo.org/record/4327005/files/dublin_postcode_boundaries.zip",
        filename="../data/dublin_postcode_boundaries.zip",
    )
    unpack_archive(
        "../data/dublin_postcode_boundaries.zip",
        "../data",
    )

postcode_boundaries = gpd.read_file(
    "../data/dublin_postcode_boundaries.geojson", driver="GeoJSON"
).to_crs(epsg=2157)


# %% [markdown]
# # Expand each Small Area to Individual Buildings

# %%
def expand_to_indiv_buildings(stock, on="value"):
    return pd.DataFrame(stock.values.repeat(stock[on], axis=0), columns=stock.columns)


dublin_indiv_buildings_at_small_area = expand_to_indiv_buildings(
    dublin_buildings_at_small_area
).drop(columns="value")

# %% [markdown]
# # Extract Dublin Small Area boundaries by pulling Dublin LA SAs from All-Of-Ireland SA Boundaries

# %%
dublin_small_areas = gpd.GeoDataFrame(
    dublin_indiv_buildings_at_small_area.merge(
        small_area_boundaries, how="left", left_on="sa_2011", right_on="SMALL_AREA"
    )
    .loc[:, ["SMALL_AREA", "geometry"]]
    .drop_duplicates()
)

# %% [markdown]
# # Link Dublin Small Areas to Dublin Postcodes

# %%
most_small_areas_linked_to_dublin_postcodes = join.centroids_within(
    dublin_small_areas, postcode_boundaries
)

# %%
missing_sas = dublin_small_areas.merge(
    small_areas_linked_to_dublin_postcodes, how="left", indicator=True
).query("`_merge` == 'left_only'")

# %%
# visually matched missing SAs to Postcodes via plot()
# missing_sas_linked = ..

# %%
small_areas_linked_to_dublin_postcodes = pd.concat(
    [most_small_areas_linked_to_dublin_postcodes, missing_sas_linked]
)

# %% [markdown]
# # Anonymise stock to Postcode level

# %%
dublin_indiv_buildings_at_postcode_level = (
    dublin_indiv_buildings_at_small_area.merge(
        small_area_boundaries, how="left", left_on="sa_2011", right_on="SMALL_AREA"
    )
    .merge(small_areas_linked_to_dublin_postcodes)
    .drop(columns=["sa_2011", "SMALL_AREA", "EDNAME", "geometry"])
)

# %% [markdown]
# # Group Buildings by Regulation Period
#
# ... as per DEAP Manual 4.2.2 Appendix S Table S1

# %%
dublin_indiv_buildings_at_postcode_level[
    "regulation_period_deap_appendix_s"
] = dublin_indiv_buildings_at_postcode_level.period_built_unstandardised.map(
    {
        "before 1919": "<1978",
        "1919 - 1945": "<1978",
        "1946 - 1960": "<1978",
        "1961 - 1970": "<1978",
        "1971 - 1980": "<1978",
        "1981 - 1990": "1983 - 1993",
        "1991 - 2000": "1994 - 1999",
        "2001 - 2005": "2000 - 2004",
        "2006 - 2011": "2005 - 2009",
    }
)

# %% [markdown]
# # Save

# %%
dublin_indiv_buildings_at_postcode_level.to_csv(
    "../data/dublin_building_stock_up_to_2011.csv", index=False
)
