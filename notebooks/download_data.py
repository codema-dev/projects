# %%
from os import mkdir
from pathlib import Path

import loguru
import pandas as pd

from berpublicsearch.download import download_berpublicsearch_parquet
from dublin_building_stock.download import (
    download,
    download_dublin_valuation_office,
    download_ber_public,
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
download_ber_public_parquet(
    data_dir=data_dir,
    email_address="rowan.molony@codema.ie",
)
# %%
download_dublin_valuation_office(data_dir=data_dir)
# %%
download(
    url="https://www.autoaddress.ie/docs/default-source/default-document-library/routingkeys_mi_itm_2016_09_29.zip",
    filepath=data_dir / "routingkeys_mi_itm_2016_09_29.zip",
)

# %% [markdown]
# # Download EPA listings of industrial sites

# %%
download(
    url="http://www.epa.ie/terminalfour/ippc/ippc-search.jsp?class-of-activity=%25&status=%25&county=Dublin&Submit=Search+by+Combination",
    filepath=data_dir / "epa_industrial_licenses.html",
)

# %%
# TODO
epa_industrial_licenses = pd.read_html(data_dir / "epa_ets_licenses.html")[0].rename(
    columns={"Operator Name": "name", "Reg No": "license"}
)

# %% [markdown]
# # Download EPA listings of ETS sites

# %%
download(
    url="http://www.epa.ie/climate/emissionstradingoverview/etscheme/accesstocurrentpermits/#d.en.64017",
    filepath=data_dir / "epa_ets_licenses.html",
)

# %%
epa_ets_licenses = (
    pd.read_html(data_dir / "epa_ets_licenses.html")[0]
    .assign(
        url_to_pdf=lambda df: "http://www.epa.ie/pubs/reports/air/etu/permit/"
        + df["Reg No"]
        + ".pdf"
    )
    .rename(columns={"Operator Name": "name", "Reg No": "license"})
)

# %%
epa_ets_dirpath = data_dir / "EPA-ETS"
mkdir(data_dir / "EPA-ETS")

# %%
for row in epa_ets_licenses.itertuples():
    filename = row.license + ".pdf"
    try:
        download(row.url_to_pdf, epa_ets_dirpath / filename)
    except:
        loguru.error("." * 10 + "ERROR" + "." * 10 + "\n\n")

# %%
