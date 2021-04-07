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


def convert_to_geodataframe(df, x, y, crs):
    geometry = gpd.points_from_xy(df[x], df[y])
    return gpd.GeoDataFrame(df, geometry=geometry, crs=crs).drop(columns=[x, y])


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
ireland_small_area_boundaries = gpd.read_file(filepath)[use_columns].assign(
    EDNAME=lambda gdf: gdf["EDNAME"]
    .str.normalize("NFKD")
    .str.encode("ascii", errors="ignore")
    .str.decode("utf-8")
    .str.replace(r"(['.])", "", regex=True)
    .str.replace(r"(-)", " ", regex=True)
)  # Remove Fadas etc for merging (... gpd.merge seems to drop fadas as side-effect)

# %%
filepath = data_dir / "dublin_small_area_boundaries.geojson"
if not filepath.exists():
    dublin_small_area_boundaries = get_geometries_within(
        ireland_small_area_boundaries.to_crs(epsg=2157),
        dublin_boundary.to_crs(epsg=2157),
    )
    dublin_small_area_boundaries.to_file(filepath, driver="GeoJSON")

dublin_small_area_boundaries = gpd.read_file(filepath)

# %%
filepath = data_dir / "dublin_electoral_district_boundaries.geojson"
if not filepath.exists():
    dublin_electoral_district_boundaries = dublin_small_area_boundaries.dissolve(
        by="EDNAME"
    ).drop(columns="SMALL_AREA")
    dublin_electoral_district_boundaries.to_file(filepath, driver="GeoJSON")

dublin_electoral_district_boundaries = gpd.read_file(filepath, driver="GeoJSON")

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
dublin_small_area_hh_boilers = extract_census_columns(
    data_dir / "SAPS2016_SA2017.csv",
    dublin_small_area_boundaries,
    column_names,
    "boiler_type",
    melt=False,
)

# %%
dublin_small_area_hh_boilers.to_csv(
    data_dir / "dublin_small_area_hh_boilers.csv",
    index=False,
)

# %% [markdown]
# # Estimate Small Area Statistics
# - BER Rating via Period Built
# - Floor Area via Closed BER Dataset
dublin_indiv_hh_age = (
    repeat_rows_on_column(dublin_small_area_hh_age, "value")
    .query("period_built != ['total']")
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
                "not stated": "unknown",
            }
        ),
        ber_kwh_per_m2_y=lambda df: df["estimated_ber"]
        .replace(
            {
                "A": 25,
                "B": 100,
                "C": 175,
                "D": 240,
                "E": 320,
                "F": 380,
                "G": 450,
                "unknown": 240,
            }
        )
        .astype(np.int32),
    )
)

# %%
dublin_indiv_hh_age.to_csv(data_dir / "dublin_indiv_hh_age.csv", index=False)

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

# %%
filepath = data_dir / "BERPublicsearch_Dublin.parquet"
if not filepath.exists():
    ber_public = dd.read_parquet(data_dir / "BERPublicsearch_parquet")
    ber_public_dublin = ber_public[
        ber_public["CountyName"].str.contains("Dublin")
    ].compute()
    ber_public_dublin.to_parquet(filepath)
else:
    ber_public_dublin = pd.read_parquet(filepath)

# %% [markdown]
# # Get Dublin BER Private Database Rows

# %%
filepath = data_dir / "BER.09.06.2020.parquet"
if not filepath.exists():
    ber_private_dublin = (
        pd.read_csv(data_dir / "BER.09.06.2020.csv")
        .query("CountyName2.str.contains('DUBLIN')")
        .merge(
            dublin_small_area_boundaries[["SMALL_AREA", "EDNAME"]],
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
        .drop(columns=["cso_small_area", "geo_small_area"])
    )
    ber_private_dublin.to_parquet(data_dir / "BER_Dublin.09.06.2020.parquet")
else:
    ber_private_dublin = pd.read_parquet(filepath)


# %% [markdown]
# # Get Valuation Office open & closed
# ... closed access includes commercially sensitive floor areas

# %%
from valuation_office_ireland.download import download_valuation_office_categories

# %%
filepath = data_dir / "valuation_office"
local_authorities = [
    "DUN LAOGHAIRE RATHDOWN CO CO",
    "DUBLIN CITY COUNCIL",
    "FINGAL COUNTY COUNCIL",
    "SOUTH DUBLIN COUNTY COUNCIL",
]
if not filepath.exists():
    download_valuation_office_categories(
        savedir=data_dir,
        local_authorities=local_authorities,
    )

    vo_public = pd.concat(
        [pd.read_csv(filepath) for filepath in filepath.glob("*.csv")]
    ).reset_index(drop=True)

# %%
dcc_vo_private = (
    pd.read_excel(
        data_dir / "DCC - Valuation Office.xlsx",
        sheet_name="Energy Calculation Sheet",
        header=3,
    )
    .assign(ID=lambda df: df["ID"].astype(str).str.extract("(\d{3,})")[0])
    .dropna(how="all", axis="rows", subset=["ID"])
    .dropna(how="all", axis="columns")
    .pipe(convert_to_geodataframe, x="X_Long", y="Y_Lat", crs="EPSG:2157")
)

# %%
dlrcc_vo_private = (
    pd.read_excel(
        data_dir / "DLRCC - Valuation Office.xlsm",
        sheet_name="Energy Demand Calculation",
        header=3,
    )
    .assign(ID=lambda df: df["ID"].astype(str).str.extract("(\d+)")[0])
    .dropna(how="all", axis="rows", subset=["ID"])
    .dropna(how="all", axis="columns")
    .pipe(convert_to_geodataframe, x="X_Long", y="Y_Lat", crs="EPSG:2157")
)

# %%
sdcc_vo_private = (
    pd.read_excel(
        data_dir / "SDCC - Valuation Office.xlsx",
        sheet_name="Energy Calculation Sheet",
        header=3,
    )
    .assign(ID=lambda df: df["ID"].astype(str).str.extract("(\d{3,})")[0])
    .dropna(how="all", axis="rows", subset=["ID"])
    .dropna(how="all", axis="columns")
    .pipe(convert_to_geodataframe, x="X_Long", y="Y_Lat", crs="EPSG:29903")
)

# %%
fcc_vo_private = (
    pd.read_excel(
        data_dir / "FCC - Valuation Office.xlsm",
        sheet_name="Energy Demand Calculation",
        header=3,
    )
    .assign(ID=lambda df: df["ID"].astype(str).str.extract("(\d{3,})")[0])
    .dropna(how="all", axis="rows", subset=["ID"])
    .dropna(how="all", axis="columns")
    .pipe(convert_to_geodataframe, x="X_Long", y="Y_Lat", crs="EPSG:2157")
)

# %%
vo_private = (
    pd.concat([dcc_vo_private, dlrcc_vo_private, sdcc_vo_private, fcc_vo_private])
    .reset_index(drop=True)
    .assign(
        heat_demand_kwh_per_year=lambda gdf: gdf["FF.3"].fillna(0)
        + gdf["Industrial Space Heating kWh"].fillna(0)
    )
    .pipe(gpd.sjoin, dublin_electoral_district_boundaries, op="within")
)

# %% [markdown]
# # Estimate Commercial HDD

# %%
dublin_electoral_district_comm_demand = (
    vo_private.groupby("EDNAME")["heat_demand_kwh_per_year"]
    .sum()
    .multiply(10 ** -6)
    .round(2)
    .fillna(0)
    .rename("residential_gwh_per_y")
)


# %% [markdown]
# # Estimate Residential HDD via Median ED Floor Area & BER Rating

# %%
median_electoral_district_floor_area = (
    ber_private_dublin.where(ber_private_dublin["Floor Total Area"] > 0, np.nan)
    .groupby("EDNAME")["Floor Total Area"]
    .median()
    .rename("median_floor_area")
)

# %%
dublin_indiv_hh_demand = dublin_indiv_hh_age.merge(
    median_electoral_district_floor_area,
    left_on="EDNAME",
    right_index=True,
    how="left",
).assign(
    ber_kwh_per_y=lambda df: df.eval("ber_kwh_per_m2_y * median_floor_area"),
)

# %%
dublin_electoral_district_hh_demand = (
    dublin_indiv_hh_demand.groupby("EDNAME")["ber_kwh_per_y"]
    .sum()
    .multiply(10 ** -6)
    .round(2)
    .fillna(0)
    .rename("commercial_gwh_per_y")
)

# %% [markdown]
# # Estimate HDD

# %%
dublin_electoral_district_hdd = (
    dublin_electoral_district_boundaries.merge(
        dublin_electoral_district_comm_demand,
        left_on="EDNAME",
        right_index=True,
        how="left",
    )
    .merge(
        dublin_electoral_district_hh_demand,
        left_on="EDNAME",
        right_index=True,
        how="left",
    )
    .assign(
        area_km2=lambda gdf: gdf.area * 10 ** -6,
        commercial_tj_per_y_km2=lambda gdf: gdf["commercial_gwh_per_y"].fillna(0)
        * 3.6
        / gdf["area_km2"],
        residential_tj_per_y_km2=lambda gdf: gdf["residential_gwh_per_y"].fillna(0)
        * 3.6
        / gdf["area_km2"],
        total_tj_per_y_km2=lambda gdf: gdf["commercial_tj_per_y_km2"]
        + gdf["residential_tj_per_y_km2"],
    )
)

# %%
dublin_electoral_district_hdd.to_file(
    data_dir / "dublin_electoral_district_hdd.geojson", driver="GeoJSON"
)

# %%
