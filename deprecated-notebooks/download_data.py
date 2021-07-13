# %%
from os import mkdir
from pathlib import Path
from urllib.request import urlretrieve

from loguru import logger
import pandas as pd
from tqdm import tqdm

from berpublicsearch.download import download_berpublicsearch_parquet
from dublin_building_stock.download import (
    download,
    download_dublin_valuation_office,
    download_ber_public,
    download_berpublicsearch_parquet
)

data_dir = Path("../data")

# %%
download(
    url="http://census.cso.ie/censusasp/saps/boundaries/Census2011_Admin_Counties_generalised20m.zip",
    filepath=data_dir / "Census2011_Admin_Counties_generalised20m.zip",
)
# %%
download(
    url="https://zenodo.org/record/4694645/files/dublin_boundary.geojson",
    filepath=data_dir / "dublin_boundary.geojson",
)
# %%
download(
    url="http://census.cso.ie/censusasp/saps/boundaries/Census2011_Small_Areas_generalised20m.zip",
    filepath=data_dir / "Census2011_Small_Areas_generalised20m.zip",
)
# %%
download(
    url="https://opendata.arcgis.com/datasets/c85e610da1464178a2cd84a88020c8e2_3.zip",
    filepath=data_dir
    / "Small_Areas_Ungeneralised_-_OSi_National_Statistical_Boundaries_-_2015-shp.zip",
)
# %%
download(
    url="https://www.cso.ie/en/media/csoie/census/census2016/census2016boundaryfiles/SAPS_2016_Glossary.xlsx",
    filepath=data_dir / "SAPS_2016_Glossary.xlsx",
)
# %%
download(
    url="https://www.cso.ie/en/media/csoie/census/census2016/census2016boundaryfiles/SAPS2016_SA2017.csv",
    filepath=data_dir / "SAPS2016_SA2017.csv",
)
# %%
download_berpublicsearch_parquet(
    savedir=data_dir,
    email_address="oisin.doherty@codema.ie",
)
# %%
download_dublin_valuation_office(data_dir=data_dir)
# %%
download(
    url="https://www.autoaddress.ie/docs/default-source/default-document-library/routingkeys_mi_itm_2016_09_29.zip",
    filepath=data_dir / "routingkeys_mi_itm_2016_09_29.zip",
)

# %%
