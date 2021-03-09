# %%
from pathlib import Path
from shutil import unpack_archive
from urllib.request import urlretrieve

import geopandas as gpd
import pandas as pd
import numpy as np

from dublin_building_stock.download import download

data_dir = Path("../data")
# %% [markdown]
# # Get 2016 Dublin Small Areas Boundaries

# %%
dublin_small_area_boundaries_filepath = data_dir / "dublin_small_area_boundaries.gpkg"
# TODO: add GPKG to Zenodo
download(
    "https://opendata.arcgis.com/datasets/c85e610da1464178a2cd84a88020c8e2_3.zip",
    dublin_small_area_boundaries_filepath,
)
dublin_small_area_boundaries = gpd.read_file(
    dublin_small_area_boundaries_filepath,
    driver="GPKG",
)


# %% [markdown]
# # Get Small Area 2016 Buildings total

# %%
small_areas_glossary_filepath = data_dir / "SAPS_2016_Glossary.xlsx"
if not small_areas_glossary_filepath.exists():
    urlretrieve(
        url="https://www.cso.ie/en/media/csoie/census/census2016/census2016boundaryfiles/SAPS_2016_Glossary.xlsx",
        filename=str(small_areas_glossary_filepath),
    )

small_area_glossary = (
    pd.read_excel(small_areas_glossary_filepath, engine="openpyxl")[
        ["Column Names", "Description of Field"]
    ]
    .set_index("Column Names")
    .to_dict()
)

small_areas_data_filepath = data_dir / "SAPS2016_SA2017.csv"
if not small_areas_data_filepath.exists():
    urlretrieve(
        url="https://www.cso.ie/en/media/csoie/census/census2016/census2016boundaryfiles/SAPS2016_SA2017.csv",
        filename=str(small_areas_data_filepath),
    )

small_area_2016_total_hh = (
    pd.read_csv(small_areas_data_filepath)
    .assign(SMALL_AREA=lambda df: df["GEOGID"].str[7:])[["T6_1_TH", "SMALL_AREA"]]
    .rename(columns={"T6_1_TH": "number_of_dwellings"})
)
dublin_small_area_2016_total_hh = small_area_2016_total_hh.merge(
    dublin_small_area_boundaries[["SMALL_AREA"]]
)

# %% [markdown]
# # Save

# %%
dublin_small_area_2016_total_hh.to_csv(
    data_dir / "number_of_dwellings_dublin_small_area_2016.csv",
    index=False,
)

# %%
small_area_resi_stock_2016.to_crs(epsg=4326).to_file(
    data_dir / "resi_census_2016_small_area_building_stock.gpkg",
    driver="GPKG",
)

# %%
