import logging
from typing import Any
from typing import List
from typing import Tuple

import fsspec
import pandas as pd
from pandas.core.reshape.merge import merge


def _clean_string(s: pd.Series):
    return (
        s.astype(str)
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .str.lower()
    )


def clean(upstream: Any, product: Any, sheet_name: str, fuel_type: str):
    sheets = pd.read_excel(
        upstream["download_seai_monitoring_and_reporting"], sheet_name=None
    )
    raw_sheet = sheets[sheet_name]
    clean_sheet = pd.DataFrame(
        {
            "pb_name": _clean_string(raw_sheet["PB Name"]),
            "postcode": _clean_string(
                raw_sheet["County"].replace({"Dublin (County)": "Co. Dublin"})
            ),
            "location": _clean_string(
                raw_sheet["Location"].str.replace("(,? ?Dublin \d+)", "", regex=True)
            ),  # remove postcodes as they are accounted for by the 'postcode' column
            "category": raw_sheet["Consumption Category"].str.lower(),
            "year": raw_sheet["Year"],
            f"{fuel_type}_kwh_per_year": raw_sheet[
                "Attributable Total Final Consumption (kWh)"
            ],
        }
    )
    clean_sheet.to_csv(product, index=False)


def merge_mprns_and_gprns(upstream: Any, product: Any) -> None:
    mprn = pd.read_csv(upstream["clean_mprns"]).rename(
        columns={"location": "mprn_location"}
    )
    gprn = pd.read_csv(upstream["clean_gprns"]).rename(
        columns={"location": "gprn_location"}
    )
    m_and_r = mprn.merge(
        gprn,
        left_on=[
            "mprn_location",
            "pb_name",
            "postcode",
            "year",
            "category",
        ],
        right_on=["gprn_location", "pb_name", "postcode", "year", "category"],
        how="outer",
        indicator=True,
    )
    m_and_r["location"] = m_and_r["mprn_location"].fillna("gprn_location")
    m_and_r["dataset"] = m_and_r["_merge"].replace(
        {"left_only": "mprn_only", "right_only": "gprn_only"}
    )
    m_and_r = m_and_r.drop(columns="_merge")
    m_and_r.to_csv(product, index=False)


def _flatten_column_names(df):
    new_names = [str(c[0]) + "_" + str(c[1]) for c in df.columns.to_flat_index()]
    df.columns = new_names
    return df


def pivot_to_one_column_per_year(m_and_r: pd.DataFrame) -> pd.DataFrame:
    m_and_r_by_year = (
        m_and_r.pivot_table(
            index="address",
            columns="year",
            values=["electricity_kwh_per_year", "gas_kwh_per_year"],
        )
        .pipe(_flatten_column_names)
        .reset_index()
    )
    return (
        m_and_r.drop(columns=["year", "electricity_kwh_per_year", "gas_kwh_per_year"])
        .merge(m_and_r_by_year)
        .drop_duplicates(subset=m_and_r_by_year.columns)
        .reset_index(drop=True)
    )
