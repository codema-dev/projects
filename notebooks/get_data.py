# %%
from pathlib import Path
from shutil import unpack_archive
from urllib.request import urlopen

import geopandas as gpd
import numpy as np
import pandas as pd
from tqdm import tqdm

data_dir = Path("../data")


def get_geometries_within(left, right):

    left_representative_point = (
        left.geometry.representative_point().rename("geometry").to_frame()
    )
    return (
        gpd.sjoin(left_representative_point, right, op="within")
        .drop(columns=["geometry", "index_right"])
        .merge(left, left_index=True, right_index=True)
        .reset_index(drop=True)
    )


def download(url, filename):
    response = urllib.urlopen(url)
    with tqdm.wrapattr(
        open(filename, "wb"),
        "write",
        miniters=1,
        desc=filename,
        total=getattr(response, "length", None),
    ) as fout:
        for chunk in response:
            fout.write(chunk)


def repeat_rows_on_column(df, on):
    return df.reindex(df.index.repeat(df[on])).drop(columns=on)


# %% [markdown]
# # Get 2011 Admin County Boundaries

filepath = data_dir / "Census2011_Admin_Counties_generalised20m"
if not filepath.exists():
    download(
        url="http://census.cso.ie/censusasp/saps/boundaries/Census2011_Admin_Counties_generalised20m.zip",
        filename=filepath.with_suffix(".zip"),
    )
    unpack_archive(
        filepath.with_suffix(".zip"),
        filepath,
    )

ireland_admin_county_boundaries = gpd.read_file(filepath)

# %% [markdown]
# # Get 2016 Dublin Small Area Boundaries
filepath = data_dir / "dublin_boundary.geojson"
if not filepath.exists():
    download(
        url="https://zenodo.org/record/4577018/files/dublin_boundary.geojson",
        filename=filepath,
    )

dublin_boundary = gpd.read_file(filepath, driver="GeoJSON")

# %% [markdown]
# # Get 2016 Dublin Small Area Boundaries

# %%
filepath = (
    data_dir
    / "Small_Areas_Ungeneralised_-_OSi_National_Statistical_Boundaries_-_2015-shp"
)
if not filepath.exists():
    download(
        url="https://opendata.arcgis.com/datasets/c85e610da1464178a2cd84a88020c8e2_3.zip",
        filename=filepath.with_suffix(".zip"),
    )
    unpack_archive(
        filepath.with_suffix(".zip"),
        filepath,
    )

use_columns = ["SMALL_AREA", "EDNAME", "geometry"]
ireland_small_area_boundaries = gpd.read_file(filepath)[use_columns]

# %%
filepath = data_dir / "dublin_small_area_boundaries.geojson"
if not filepath.exists():
    dublin_small_area_boundaries = get_geometries_within(
        ireland_small_area_boundaries.to_crs(epsg=2157),
        dublin_boundary.to_crs(epsg=2157),
    )
    dublin_small_area_boundaries.to_file(filepath, driver="GeoJSON")

dublin_small_area_boundaries = gpd.read_file(filepath)

# %% [markdown]
# # Get 2016 Dublin Small Area Statistics

# %%
filepath = data_dir / "SAPS_2016_Glossary.xlsx"
if not filepath.exists():
    download(
        url="https://www.cso.ie/en/media/csoie/census/census2016/census2016boundaryfiles/SAPS_2016_Glossary.xlsx",
        filename=filepath,
    )

# %%
filepath = data_dir / "SAPS2016_SA2017.csv"
if not filepath.exists():
    download(
        url="https://www.cso.ie/en/media/csoie/census/census2016/census2016boundaryfiles/SAPS2016_SA2017.csv",
        filename=str(filepath),
        reporthook=pb.update_to,
    )
    unpack_archive(
        filepath.with_suffix(".zip"),
        filepath,
    )

# %%
def extract_census_columns(filepath, boundary, column_names, category, melt=True):
    use_columns = ["SMALL_AREA"] + list(column_names.values())

    if melt:
        extracted_columns = (
            pd.read_csv(filepath)
            .rename(columns=column_names)
            .assign(SMALL_AREA=lambda df: df["GEOGID"].str[7:])
            .loc[:, use_columns]
            .merge(boundary[["SMALL_AREA", "EDNAME"]])
            .melt(id_vars=["EDNAME", "SMALL_AREA"], var_name=category)
            .assign(value=lambda df: df["value"].astype(np.int32))
        )
    else:
        extracted_columns = (
            pd.read_csv(filepath)
            .rename(columns=column_names)
            .assign(SMALL_AREA=lambda df: df["GEOGID"].str[7:])
            .loc[:, use_columns]
            .merge(boundary["SMALL_AREA"])
        )

    return extracted_columns


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
dublin_small_area_hh_age = extract_census_columns(
    data_dir / "SAPS2016_SA2017.csv",
    dublin_small_area_boundaries,
    column_names,
    "period_built",
)

# %%
column_names = {
    "T6_1_HB_H": "House/Bungalow",
    "T6_1_FA_H": "Flat/Apartment",
    "T6_1_BS_H": "Bed-Sit",
    "T6_1_CM_H": "Caravan/Mobile home",
    "T6_1_CM_H": "Not Stated",
    "T6_1_TH": "Total",
}
dublin_small_area_hh_types = extract_census_columns(
    data_dir / "SAPS2016_SA2017.csv",
    dublin_small_area_boundaries,
    column_names,
    "hh_type",
)

# %%
column_names = {
    "T6_5_NCH": "No central heating",
    "T6_5_OCH": "Oil",
    "T6_5_NGCH": "Natural gas",
    "T6_5_ECH": "Electricity",
    "T6_5_CCH": "Coal (incl. anthracite)",
    "T6_5_PCH": "Peat (incl. turf)",
    "T6_5_LPGCH": "Liquid petroleum gas (LPG)",
    "T6_5_WCH": "Wood (incl. wood pellets)",
    "T6_5_OTH": "Other",
    "T6_5_NS": "Not stated",
    "T6_5_T": "Total",
}
dublin_small_area_boiler_types = extract_census_columns(
    data_dir / "SAPS2016_SA2017.csv",
    dublin_small_area_boundaries,
    column_names,
    "boiler_type",
)

# %% [markdown]
# # Estimate Small Area Statistics
# - BER Rating via Period Built
# - Floor Area via Closed BER Dataset
dublin_small_area_hh_age_indiv = (
    repeat_rows_on_column(dublin_small_area_hh_age, "value")
    .query("period_built != ['total', 'not stated']")
    .assign(
        estimated_ber=lambda df: df["period_built"].replace(
            {
                "before 1919": "E",
                "1919 - 1945": "E",
                "1946 - 1960": "E",
                "1961 - 1970": "D",
                "1971 - 1980": "D",
                "1981 - 1990": "D",
                "1991 - 2000": "D",
                "2001 - 2010": "C",
                "2011 or later": "A",
            }
        )
    )
)

# %%
dublin_small_area_hh_age_indiv.to_csv(
    data_dir / "dublin_small_area_hh_age_indiv.csv", index=False
)

# %% [markdown]
# # Get Dublin BER Public Database Rows

# %%
from berpublicsearch.download import download_berpublicsearch_parquet
import dask.dataframe as dd

# %%
filepath = data_dir / "BERPublicsearch_parquet"
if not filepath.exists():
    download_berpublicsearch_parquet(
        email_address="rowan.molony@codema.ie",
        savedir=data_dir,
    )

ber_public = dd.read_parquet(filepath)

# %%
ber_public_dublin = ber_public[
    ber_public["CountyName"].str.contains("Dublin")
].compute()

# %%
ber_public_dublin.to_parquet(data_dir / "BERPublicsearch_Dublin.parquet")


# %% [markdown]
# # Get Dublin BER Private Database Rows

# %%
ber_private_dublin = (
    pd.read_csv(data_dir / "BER.09.06.2020.csv")
    .query("CountyName2.str.contains('DUBLIN')")
    .merge(
        dublin_small_area_boundaries["SMALL_AREA"],
        left_on="cso_small_area",
        right_on="SMALL_AREA",
    )  # filter out invalide Small Areas
    .assign(
        EDNAME=lambda df: df["ED_Name"].str.title(),
        BERBand=lambda df: df["Energy Rating"].str[0],
        period_built=lambda df: pd.cut(
            df["Year of construction"],
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
    )
    .drop(columns=["cso_small_area", "geo_small_area", "ED_Name"])
)

# %%
ber_private_dublin.to_csv(data_dir / "BER_Dublin.09.06.2020.csv", index=False)

# %%
