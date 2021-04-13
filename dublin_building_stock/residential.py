import dask.dataframe as dd
import geopandas as gpd
import numpy as np
import pandas as pd


def _repeat_rows_on_column(df, on):
    return df.reindex(df.index.repeat(df[on])).drop(columns=on)


def _extract_census_columns(filepath, boundary, column_names, category, melt=True):
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


def create_census_2016_hh_age(data_dir, dublin_small_area_boundaries_2016):
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
    census_2016_hh_age = _extract_census_columns(
        data_dir / "SAPS2016_SA2017.csv",
        dublin_small_area_boundaries_2016,
        column_names,
        "period_built",
    )
    census_2016_hh_age.to_csv(data_dir / "census_2016_hh_age.csv", index=False)


def create_census_2016_hh_type(data_dir, dublin_small_area_boundaries_2016):
    column_names = {
        "T6_1_HB_H": "House/Bungalow",
        "T6_1_FA_H": "Flat/Apartment",
        "T6_1_BS_H": "Bed-Sit",
        "T6_1_CM_H": "Caravan/Mobile home",
        "T6_1_CM_H": "Not Stated",
        "T6_1_TH": "Total",
    }
    census_2016_hh_type = _extract_census_columns(
        data_dir / "SAPS2016_SA2017.csv",
        dublin_small_area_boundaries_2016,
        column_names,
        "hh_type",
    )
    census_2016_hh_type.to_csv(data_dir / "census_2016_hh_type.csv", index=False)


def create_census_2016_hh_boilers(data_dir, dublin_small_area_boundaries_2016):
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
    census_2016_hh_boilers = _extract_census_columns(
        data_dir / "SAPS2016_SA2017.csv",
        dublin_small_area_boundaries_2016,
        column_names,
        "boiler_type",
        melt=False,
    )
    census_2016_hh_boilers.to_csv(data_dir / "census_2016_hh_boilers.csv", index=False)


def create_census_2016_hh_age_indiv(data_dir, census_2016_hh_age):
    return (
        _repeat_rows_on_column(census_2016_hh_age, "value")
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


def create_census_2011_small_area_hhs(data_dir, dublin_small_area_boundaries_2011):
    filepaths = [
        pd.read_csv(
            data_dir / "Census-2011-crosstabulations" / f"{local_authority}_SA_2011.csv"
        )
        for local_authority in ["DCC", "DLR", "FCC", "SD"]
    ]
    census_2011_small_area_hhs = (
        pd.concat(filepaths)
        .query("`sa_2011` != ['Dublin City', 'South Dublin']")
        .query("`period_built_unstandardised` != ['Total', 'All Houses']")
        .replace({">3": 1, "<3": 1, ".": np.nan})
        .dropna(subset=["value"])
        .rename(
            columns={
                "sa_2011": "SMALL_AREA",
                "period_built_unstandardised": "period_built",
            }
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
    census_2011_small_area_hhs.to_csv(
        data_dir / "census_2011_small_area_hhs.csv", index=False
    )


def create_census_2011_hh_indiv(data_dir, census_2011_small_area_hhs):
    census_2011_hh_indiv = (
        _repeat_rows_on_column(census_2011_small_area_hhs, "value")
        .query("period_built != ['total']")
        .assign(
            category_id=lambda df: df.groupby(
                ["EDNAME", "dwelling_type", "period_built"]
            )
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
    census_2011_hh_indiv.to_csv(data_dir / "census_2011_hh_indiv.csv", index=False)


def create_dublin_ber_public(data_dir):
    ber_public = dd.read_parquet(data_dir / "BERPublicsearch_parquet")
    dublin_ber_public = ber_public[
        ber_public["CountyName"].str.contains("Dublin")
    ].compute()
    dublin_ber_public.to_parquet(data_dir / "dublin_ber_public.parquet")


def create_dublin_ber_private(data_dir, dublin_small_area_boundaries_2016):
    dublin_ber_private = (
        pd.read_csv(data_dir / "BER.09.06.2020.csv")
        .query("CountyName2.str.contains('DUBLIN')")
        .merge(
            dublin_small_area_boundaries_2016[["SMALL_AREA", "EDNAME"]],
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
    dublin_ber_private.to_parquet(data_dir / "dublin_ber_private.parquet")


def create_latest_stock(data_dir, census_2011_hh_indiv, dublin_ber_private):
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
    dublin_indiv_hh_including_unmatched = census_2011_hh_indiv.merge(
        dublin_ber_private[right_columns],
        on=["EDNAME", "dwelling_type", "period_built", "category_id"],
        how="outer",
        indicator=True,
        suffixes=["_2011", "_2016"],
    )

    not_stated_hhs = dublin_indiv_hh_including_unmatched.query(
        "period_built == 'not stated' or dwelling_type == 'Not stated'"
    )
    unmatched_hhs = dublin_indiv_hh_including_unmatched.query(
        "`Year of construction` <= 2011"
    ).query("_merge != 'both'")

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
    dublin_indiv_hh.to_csv(data_dir / "dublin_indiv_hh.csv", index=False)
