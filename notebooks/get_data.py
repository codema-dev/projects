# %%
from collections import defaultdict
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
    response = urlopen(url)
    with tqdm.wrapattr(
        open(str(filename), "wb"),
        "write",
        miniters=1,
        desc=str(filename),
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
# # Get Dublin Boundary
filepath = data_dir / "dublin_boundary.geojson"
if not filepath.exists():
    download(
        url="https://zenodo.org/record/4577018/files/dublin_boundary.geojson",
        filename=filepath,
    )

dublin_boundary = gpd.read_file(filepath, driver="GeoJSON")

# %% [markdown]
# # Get 2011 Dublin Small Area Boundaries

# %%
filepath = data_dir / "Census2011_Small_Areas_generalised20m"
if not filepath.exists():
    download(
        url="http://census.cso.ie/censusasp/saps/boundaries/Census2011_Small_Areas_generalised20m.zip",
        filename=filepath.with_suffix(".zip"),
    )
    unpack_archive(
        filepath.with_suffix(".zip"),
        filepath,
    )

use_columns = ["SMALL_AREA", "EDNAME", "geometry"]
ireland_small_area_boundaries_2011 = gpd.read_file(filepath)[use_columns]

# %%
filepath = data_dir / "dublin_small_area_boundaries_2011.geojson"
if not filepath.exists():
    dublin_small_area_boundaries_2011 = get_geometries_within(
        ireland_small_area_boundaries_2011.to_crs(epsg=2157),
        dublin_boundary.to_crs(epsg=2157),
    )
    dublin_small_area_boundaries_2011.to_file(filepath, driver="GeoJSON")

dublin_small_area_boundaries_2011 = gpd.read_file(filepath)


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
ireland_small_area_boundaries_2016 = gpd.read_file(filepath)[use_columns]

# %%
filepath = data_dir / "dublin_small_area_boundaries_2016.geojson"
if not filepath.exists():
    dublin_small_area_boundaries_2016 = get_geometries_within(
        ireland_small_area_boundaries.to_crs(epsg=2157),
        dublin_boundary.to_crs(epsg=2157),
    )
    dublin_small_area_boundaries_2016.to_file(filepath, driver="GeoJSON")

dublin_small_area_boundaries_2016 = gpd.read_file(filepath)

# %% [markdown]
# # Get Dublin Electoral District Boundaries

# %%
filepath = data_dir / "Census2011_Electoral_Divisions_generalised20m.geojson"
if not filepath.exists():
    download(
        url="http://census.cso.ie/censusasp/saps/boundaries/Census2011_Electoral_Divisions_generalised20m.zip",
        filename=filepath.with_suffix(".zip"),
    )
    unpack_archive(
        filepath.with_suffix(".zip"),
        filepath,
    )

ireland_electoral_divisions = gpd.read_file(filepath)[["EDNAME", "geometry"]]

# %%
filepath = data_dir / "dublin_electoral_divisions.geojson"
if not filepath.exists():
    dublin_electoral_divisions = get_geometries_within(
        ireland_electoral_divisions.to_crs(epsg=2157),
        dublin_boundary.to_crs(epsg=2157),
    )
    dublin_electoral_divisions.to_file(filepath, driver="GeoJSON")

dublin_electoral_divisions = gpd.read_file(filepath)


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
    dublin_small_area_boundaries_2016,
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
    dublin_small_area_boundaries_2016,
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
    dublin_small_area_boundaries_2016,
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
# # Get Private Census 2011 Cross-Tabulated Statistics
# ... period built | dwelling type at Small Area level

# %%
dublin_small_area_crosstab_2011 = (
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
    .rename(
        columns={"sa_2011": "SMALL_AREA", "period_built_unstandardised": "period_built"}
    )
    .merge(dublin_small_area_boundaries_2011[["SMALL_AREA", "EDNAME"]])
    .assign(
        value=lambda df: df["value"].astype(np.int32),
        SMALL_AREA=lambda df: df["SMALL_AREA"].str.replace(r"_", r"/"),
        period_built=lambda df: df["period_built"]
        .str.lower()
        .str.replace("2006 or later", "2006 - 2011"),
        dwelling_type=lambda df: df["dwelling_type_unstandardised"].replace(
            {
                "Flat/apartment in a purpose-built block": "Apartment",
                "Flat/apartment in a converted house or commercial building": "Apartment",
                "Bed-sit": "Apartment",
            }
        ),
    )
    .reset_index(drop=True)
    .drop(columns=["dwelling_type_unstandardised"])
)

# %%
dublin_indiv_hh_2011 = (
    repeat_rows_on_column(dublin_small_area_crosstab_2011, "value")
    .query("period_built != ['total']")
    .assign(
        category_id=lambda df: df.groupby(["EDNAME", "dwelling_type", "period_built"])
        .cumcount()
        .apply(lambda x: x + 1),
        EDNAME=lambda df: df["EDNAME"]
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .str.replace(r"[-]", " ", regex=True)
        .str.replace(r"[,'.]", "", regex=True)
        .str.lower(),
    )
)

# %%
dublin_indiv_hh_2011.to_csv(data_dir / "dublin_indiv_hh_2011.csv", index=False)

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
filepath = data_dir / "BER_Dublin.09.06.2020.parquet"
if not filepath.exists():
    ber_private_dublin = (
        pd.read_csv(data_dir / "BER.09.06.2020.csv")
        .query("CountyName2.str.contains('DUBLIN')")
        .merge(
            dublin_small_area_boundaries_2011[["SMALL_AREA", "EDNAME"]],
            left_on="cso_small_area",
            right_on="SMALL_AREA",
        )  # filter out invalide Small Areas
        .assign(
            BERBand=lambda df: df["Energy Rating"].str[0],
            period_built=lambda df: pd.cut(
                df["Year of construction"],
                bins=[
                    -np.inf,
                    1919,
                    1945,
                    1960,
                    1970,
                    1980,
                    1990,
                    2000,
                    2005,
                    2012,
                    np.inf,
                ],
                labels=[
                    "before 1919",
                    "1919 - 1945",
                    "1946 - 1960",
                    "1961 - 1970",
                    "1971 - 1980",
                    "1981 - 1990",
                    "1991 - 2000",
                    "2001 - 2005",
                    "2006 - 2011",
                    "after 2012",
                ],
            ),
            dwelling_type=lambda df: df["Dwelling type description"].replace(
                {
                    "Grnd floor apt.": "Apartment",
                    "Mid floor apt.": "Apartment",
                    "Top floor apt.": "Apartment",
                    "Maisonette": "Apartment",
                    "Apt.": "Apartment",
                    "Det. house": "Detached house",
                    "Semi-det. house": "Semi-detached house",
                    "House": "Semi-detached house",
                    "Mid terrc house": "Terraced house",
                    "End terrc house": "Terraced house",
                    None: "Not stated",
                }
            ),
            category_id=lambda df: df.groupby(
                ["EDNAME", "dwelling_type", "period_built"]
            )
            .cumcount()
            .apply(lambda x: x + 1),
            total_floor_area=lambda df: np.where(
                df["Floor Total Area"] > 5, df["Floor Total Area"], np.nan
            ),
            EDNAME=lambda df: df["EDNAME"]
            .str.normalize("NFKD")
            .str.encode("ascii", errors="ignore")
            .str.decode("utf-8")
            .str.replace(r"[-]", " ", regex=True)
            .str.replace(r"[,'.]", "", regex=True)
            .str.lower(),
        )
        .drop(columns=["cso_small_area", "geo_small_area"])
    )
    ber_private_dublin.to_parquet(filepath)
else:
    ber_private_dublin = pd.read_parquet(filepath)

# %% [markdown]
# # Get Dwelling Type | Period Built Stock at ED Level
# ... by pulling buildings built post 2010 from the private BER

# %%
right_columns = [
    "EDNAME",
    "dwelling_type",
    "period_built",
    "category_id",
    "SMALL_AREA",
    "Year of construction",
    "total_floor_area",
    "BERBand",
]
dublin_indiv_hh_including_unmatched = dublin_indiv_hh_2011.merge(
    ber_private_dublin[right_columns],
    on=["EDNAME", "dwelling_type", "period_built", "category_id"],
    how="outer",
    indicator=True,
    suffixes=["_2011", "_2016"],
)

# %%
not_stated_hhs = dublin_indiv_hh_including_unmatched.query(
    "period_built == 'not stated' or dwelling_type == 'Not stated'"
)
unmatched_hhs = dublin_indiv_hh_including_unmatched.query(
    "`Year of construction` <= 2011"
).query("_merge != 'both'")

# %%
dublin_indiv_hh = (
    dublin_indiv_hh_including_unmatched.query("index not in @unmatched_hhs.index")
    .assign(
        category_floor_area=lambda df: df.groupby(["EDNAME", "dwelling_type"])[
            "total_floor_area"
        ]
        .apply(lambda x: x.fillna(x.mean()))
        .round(),
        dwelling_type_floor_area=lambda df: df.groupby("dwelling_type")[
            "total_floor_area"
        ]
        .apply(lambda x: x.fillna(x.mean()))
        .round(),
        ed_floor_area=lambda df: df.groupby("EDNAME")["total_floor_area"]
        .apply(lambda x: x.fillna(x.mean()))
        .round(),
        estimated_floor_area=lambda df: df["total_floor_area"]
        .fillna(df["category_floor_area"])
        .fillna(df["dwelling_type_floor_area"])
        .fillna(df["ed_floor_area"]),
        estimated_ber=lambda df: df["period_built"].replace(
            {
                "before 1919": "E",
                "1919 - 1945": "E",
                "1946 - 1960": "E",
                "1961 - 1970": "D",
                "1971 - 1980": "D",
                "1981 - 1990": "D",
                "1991 - 2000": "D",
                "2001 - 2005": "C",
                "2006 - 2011": "B",
                "after 2012": "A",
                "not stated": "unknown",
            }
        ),
        inferred_ber=lambda df: np.where(
            df["BERBand"].isnull(),
            df["estimated_ber"],
            df["BERBand"],
        ),
        energy_kwh_per_m2_y=lambda df: df["estimated_ber"]
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
        heating_kwh_per_m2_y=lambda df: df["energy_kwh_per_m2_y"] * 0.8,
        heating_kwh_per_y=lambda df: df["heating_kwh_per_m2_y"]
        * df["estimated_floor_area"],
    )
    .drop(
        columns=[
            "total_floor_area",
            "category_floor_area",
            "dwelling_type_floor_area",
            "ed_floor_area",
        ]
    )
)

# %%
dublin_indiv_hh.to_csv(data_dir / "dublin_indiv_hh.csv", index=False)


# %% [markdown]
# # Get Valuation Office Bencmarks
# ... can use the benchmarks to estimate commercial demands via floor areas

# %%
dirpath = data_dir / "benchmarks" / "uses"
benchmark_uses = defaultdict()
for filepath in dirpath.glob("*.txt"):
    with open(filepath, "r") as file:
        benchmark_uses[filepath.stem] = [line.rstrip() for line in file]


uses_benchmarks = {i: k for k, v in benchmark_uses.items() for i in v}

# %%
vo_benchmarks = pd.read_excel(data_dir / "benchmarks" / "benchmarks.xlsx").dropna(
    how="all", axis="columns"
)

# %% [markdown]
# # Get Valuation Office public

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

vo_public = (
    pd.concat(
        [pd.read_csv(filepath) for filepath in filepath.glob("*.csv")]
    )
    .reset_index(drop=True)
    .assign(
        Benchmark=lambda gdf: gdf["Property Use"].map(uses_benchmarks),
    )  # link uses to benchmarks so can merge on common benchmarks
    .merge(vo_benchmarks, how="left")
)

# %% [markdown]
# # Get Valuation Office private
# ... closed access includes commercially sensitive floor areas

# %%
dcc_vo_private = (
    pd.read_excel(
        data_dir / "Valuation-Office-2015" / "DCC.xlsx",
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
        data_dir / "Valuation-Office-2015" / "DLRCC.xlsm",
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
        data_dir / "Valuation-Office-2015" / "SDCC.xlsx",
        sheet_name="Energy Calculation Sheet",
        header=3,
    )
    .assign(ID=lambda df: df["ID"].astype(str).str.extract("(\d{3,})")[0])
    .dropna(how="all", axis="rows", subset=["ID"])
    .dropna(how="all", axis="columns")
    .pipe(convert_to_geodataframe, x="X_Long", y="Y_Lat", crs="EPSG:29903")
    .to_crs(epsg=2157)
)

# %%
fcc_vo_private = (
    pd.read_excel(
        data_dir / "Valuation-Office-2015" / "FCC.xlsm",
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
        Benchmark=lambda gdf: gdf["Property Use"].map(uses_benchmarks),
    )  # link uses to benchmarks so can merge on common benchmarks
    .merge(vo_benchmarks, how="left")
    .assign(
        bounded_area_m2=lambda df: np.where(
            (df["Area (m2)"] > 5) & (df["Area (m2)"] < df["Area Upper Bound [m²]"]),
            df["Area (m2)"],
            np.nan,
        ),  # Remove all areas outside of 5 <= area <= Upper Bound
        to_gia=lambda df: df["Basis for Area Calculation"]
        .replace(
            {
                "GIA": 1,
                "GEA": 0.95,
                "NIA": 1.25,
            }
        )
        .astype("float16"),
        area_conversion_factors=lambda df: df["to_gia"] * df["GIA to Sales"].fillna(1),
    )
    .pipe(gpd.sjoin, dublin_small_area_boundaries_2011, op="within")
    .rename(
        columns={
            "Typical fossil fuel [kWh/m²y]": "typical_ff",
            "Industrial space heat [kWh/m²y]": "industrial_sh",
        }
    )
    .assign(
        latitude=lambda gdf: gdf.to_crs(epsg=4326).geometry.y,
        longitude=lambda gdf: gdf.to_crs(epsg=4326).geometry.x,
        inferred_area_m2=lambda df: np.round(
            df["bounded_area_m2"].fillna(df["Typical Area [m²]"])
            * df["area_conversion_factors"]
        ),
        area_is_estimated=lambda df: df["bounded_area_m2"].isnull(),
        heating_mwh_per_year=lambda df: np.round(
            (
                df["typical_ff"].fillna(0) * df["inferred_area_m2"]
                + df["industrial_sh"].fillna(0) * df["inferred_area_m2"]
            )
            * 10 ** -3
        ),
        category_area_band_m2=lambda gdf: gdf.groupby("Benchmark")[
            "inferred_area_m2"
        ].transform(lambda x: str(x.min()) + " - " + str(x.max())),
        Use=lambda gdf: gdf["Property Use"].str.title(),
        typical_ff=lambda gdf: gdf["typical_ff"]
        .replace({0: np.nan})
        .astype(str)
        .replace({"nan": ""}),
        industrial_sh=lambda gdf: gdf["industrial_sh"]
        .replace({0: np.nan})
        .astype(str)
        .replace({"nan": ""}),
    )
    .loc[
        :,
        [
            "ID",
            "Benchmark",
            "Use",
            "inferred_area_m2",
            "area_is_estimated",
            "Industrial",
            "heating_mwh_per_year",
            "typical_ff",
            "industrial_sh",
            "category_area_band_m2",
            "Area (m2)",
            "latitude",
            "longitude",
            "geometry",
        ],
    ]
)

# %%
vo_private.to_file(data_dir / "vo_private.geojson", driver="GeoJSON")

# %%
vo_private.sort_values("heating_mwh_per_year", ascending=False).to_csv(
    data_dir / "vo_private.csv", index=False
)


# %% [markdown]
# # Estimate Commercial HDD
dublin_small_area_comm_demand = (
    vo_private.groupby("SMALL_AREA")["heating_kwh_per_year"]
    .sum()
    .multiply(10 ** -6)
    .round(2)
    .fillna(0)
    .rename("commercial_gwh_per_y")
    .reset_index()
)

# %% [markdown]
# # Estimate Residential HDD
dublin_small_area_hh_demand = (
    dublin_indiv_hh.groupby("SMALL_AREA_2011")["heating_kwh_per_y"]
    .sum()
    .multiply(10 ** -6)
    .round(2)
    .fillna(0)
    .rename("residential_gwh_per_y")
    .reset_index()
)

# %% [markdown]
# # Estimate HDD

# %%
dublin_small_area_hdd = (
    dublin_small_area_boundaries_2011.merge(
        dublin_small_area_comm_demand,
        how="left",
    )
    .merge(
        dublin_small_area_hh_demand,
        how="left",
        left_on="SMALL_AREA",
        right_on="SMALL_AREA_2011",
    )
    .assign(
        total_gwh_per_y=lambda gdf: gdf["commercial_gwh_per_y"].fillna(0)
        + gdf["residential_gwh_per_y"],
        area_km2=lambda gdf: gdf.area * 10 ** -6,
        commercial_tj_per_y_km2=lambda gdf: np.round(
            gdf["commercial_gwh_per_y"].fillna(0) * 3.6 / gdf["area_km2"], 2
        ),
        residential_tj_per_y_km2=lambda gdf: np.round(
            gdf["residential_gwh_per_y"].fillna(0) * 3.6 / gdf["area_km2"], 2
        ),
        total_tj_per_y_km2=lambda gdf: gdf["commercial_tj_per_y_km2"]
        + gdf["residential_tj_per_y_km2"],
    )
)

# %%
dublin_small_area_hdd.to_file(
    data_dir / "dublin_small_area_hdd.geojson", driver="GeoJSON"
)

# %%
