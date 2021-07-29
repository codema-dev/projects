from typing import List
from typing import Tuple

import fsspec
import pandas as pd
from pandas.core.reshape.merge import merge


def read_excel_sheets(url: str) -> Tuple[List[str], List[pd.DataFrame]]:
    with fsspec.open(url) as f:
        return pd.read_excel(f, sheet_name=None)


def _clean_string(s: pd.Series):
    return (
        s.astype(str)
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .str.lower()
    )


def clean(dfs: pd.DataFrame, sheet_name: str, demand_type: str):
    demand_column_name = f"{demand_type}_kwh_per_year"
    clean_df = dfs[sheet_name].copy()
    clean_df["pb_name"] = _clean_string(clean_df["PB Name"])
    clean_df["postcode"] = _clean_string(
        clean_df["County"].replace({"Dublin (County)": "Co. Dublin"})
    )
    clean_df["location"] = _clean_string(
        clean_df["Location"].str.replace("(,? ?Dublin \d+)", "", regex=True)
    )  # remove postcodes as accounted for by 'postcode' column
    clean_df["address"] = (
        clean_df["pb_name"]
        + " "
        + clean_df["location"]
        + " "
        + clean_df["postcode"]
        + " "
        + "ireland"
    )
    clean_df = clean_df.rename(
        columns={
            "Attributable Total Final Consumption (kWh)": demand_column_name,
            "Consumption Category": "category",
            "Year": "year",
        }
    )
    return clean_df.loc[
        :,
        [
            "pb_name",
            "address",
            "postcode",
            "category",
            "year",
            demand_column_name,
        ],
    ]


def merge_sheets(mprn: pd.DataFrame, gprn: pd.DataFrame) -> pd.DataFrame:
    merged = pd.merge(left=mprn, right=gprn, how="outer", indicator=True)
    merged["dataset"] = merged["_merge"].replace(
        {"left_only": "mprn_only", "right_only": "gprn_only"}
    )
    return merged.drop(columns="_merge")


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
