from csv import QUOTE_NONE
import json
from os import PathLike
from pathlib import Path
from typing import Any
from typing import Dict
from zipfile import ZipFile

import dask.dataframe as dd
import numpy as np
import pandas as pd
import requests


def download_building_energy_ratings(product: PathLike) -> None:
    cookies = {
        "ASP.NET_SessionId": "gvb1njrssax1kcmjyzatuf3x",
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://ndber.seai.ie",
        "Connection": "keep-alive",
        "Referer": "https://ndber.seai.ie/BERResearchTool/ber/search.aspx",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "DNT": "1",
        "Sec-GPC": "1",
    }

    data = {
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__VIEWSTATE": "/wEPDwULLTE2MDEwODU4NjAPFgIeE1ZhbGlkYXRlUmVxdWVzdE1vZGUCARYCZg9kFgICAw9kFgICAw8WAh4FY2xhc3MFC21haW53cmFwcGVyFgICBQ8PFgIeB1Zpc2libGVnZGRk/o+nX293q8AvTPN9mLTMfC2ZSlFasqiSYYRZIXBq1B8=",
        "__VIEWSTATEGENERATOR": "AFB8016F",
        "__SCROLLPOSITIONX": "0",
        "__SCROLLPOSITIONY": "280",
        "__EVENTVALIDATION": "/wEdAAPEuLD//Lrnct58vIDU2Hx9Xw+uRy4COswXgj8OGX6Nym8pc/9FsasLndkJePd0e319WvmW++umNulm4SeWaVFh1FOy2200t0nXvcBQEo5kHw==",
        "ctl00$DefaultContent$BERSearch$dfExcelDownlaod$DownloadAllData": "Download All Data",
    }

    response = requests.post(
        "https://ndber.seai.ie/BERResearchTool/ber/search.aspx",
        headers=headers,
        cookies=cookies,
        data=data,
    )

    with open(product, "wb") as f:
        for chunk in response.iter_content(chunk_size=4096):
            f.write(chunk)


def unzip_building_energy_ratings(
    product: PathLike, upstream: Dict[str, PathLike]
) -> None:
    zf = ZipFile(upstream["download_building_energy_ratings"])
    zf.extractall(product)


def save_selected_columns_as_parquet(
    product: Any, upstream: Any, names: Dict[str, str], dtypes: Dict[str, str]
) -> None:
    filepath = Path(upstream["unzip_building_energy_ratings"]) / "BERPublicsearch.txt"
    building_energy_ratings = pd.read_csv(
        filepath,
        sep="\t",
        encoding="latin-1",
        quoting=QUOTE_NONE,
        usecols=names.keys(),
        dtype=dtypes,
    ).rename(columns=names)
    building_energy_ratings.to_parquet(product)


def extract_buildings_meeting_conditions(product: Any, upstream: Any) -> None:
    buildings = pd.read_parquet(upstream["save_selected_columns_as_parquet"])
    dublin_small_area_ids = pd.read_csv(
        upstream["download_dublin_small_area_ids"]
    ).squeeze()

    conditions = [
        "type_of_rating != 'Provisional    '",
        "ground_floor_area > 0 and ground_floor_area < 1000",
        "ground_floor_height > 0",
        "living_area_percent > 5 or living_area_percent < 90",
        "main_sh_boiler_efficiency > 19 or main_sh_boiler_efficiency < 600",
        "main_hw_boiler_efficiency > 19 or main_hw_boiler_efficiency < 320",
        "main_sh_boiler_efficiency_adjustment_factor > 0.7",
        "main_hw_boiler_efficiency_adjustment_factor > 0.7",
        "declared_loss_factor < 20",
        "thermal_bridging_factor > 0 or thermal_bridging_factor <= 0.15",
        "small_area in @dublin_small_area_ids",
    ]
    query_str = " and ".join(["(" + c + ")" for c in conditions])
    buildings_meeting_conditions = buildings.query(query_str)

    total_dublin_buildings = len(buildings[buildings.countyname.str.contains("Dublin")])
    print(f"Buildings in Dublin: {total_dublin_buildings}")
    total_buildings_meeting_conditions = len(buildings_meeting_conditions)
    print(f"Buildings meeting conditions: {total_buildings_meeting_conditions}")

    buildings_meeting_conditions.to_parquet(product)


def extract_dublin_census_buildings(product: Any, upstream: Any) -> None:
    census = pd.read_csv(upstream["download_census_building_ages"])
    dublin_small_area_ids = pd.read_csv(
        upstream["download_dublin_small_area_ids"]
    ).squeeze()
    dublin_census = census.query("small_area in @dublin_small_area_ids")
    dublin_census.to_parquet(product)


def fill_census_with_bers(product: Any, upstream: Any) -> None:
    census = pd.read_parquet(upstream["extract_dublin_census_buildings"])
    bers = pd.read_parquet(upstream["extract_buildings_meeting_conditions"])
    with open(upstream["download_small_area_electoral_district_id_map"], "r") as f:
        small_area_electoral_district_id_map = json.load(f)

    merge_columns = ["small_area", "period_built"]

    census["id"] = census.groupby(merge_columns).cumcount().apply(lambda x: x + 1)

    bers["period_built"] = pd.cut(
        bers["year_of_construction"],
        bins=[
            -np.inf,
            1919,
            1945,
            1960,
            1970,
            1980,
            1990,
            2000,
            2011,
            np.inf,
        ],
        labels=[
            "PRE19",
            "19_45",
            "46_60",
            "61_70",
            "71_80",
            "81_90",
            "91_00",
            "01_10",
            "11L",
        ],
    )
    bers["id"] = bers.groupby(merge_columns).cumcount().apply(lambda x: x + 1)

    before_2016 = census.merge(
        bers.query("year_of_construction < 2016"),
        on=["small_area", "period_built", "id"],
        how="left",
    )
    after_2016 = bers.query("year_of_construction >= 2016")

    census_with_bers = pd.concat([before_2016, after_2016]).reset_index(drop=True)

    census_with_bers["cso_ed_id"] = census_with_bers["small_area"].map(
        small_area_electoral_district_id_map
    )

    census_with_bers.to_parquet(product)


def _get_mode_or_first_occurence(srs: pd.Series) -> str:
    m = pd.Series.mode(srs)
    return m.values[0] if not m.empty else np.nan


def _get_aggregation_operations(df):
    numeric_operations = {c: "median" for c in df.select_dtypes("number").columns}
    categorical_operations = {
        c: _get_mode_or_first_occurence
        for c in set(
            df.select_dtypes("object").columns.tolist()
            + df.select_dtypes("string").columns.tolist()
            + df.select_dtypes("category").columns.tolist()
        )
    }
    return {**numeric_operations, **categorical_operations}


def create_archetypes(product: Any, upstream: Any) -> None:
    buildings = pd.read_parquet(upstream["fill_census_with_bers"])

    dirpath = Path(product)
    dirpath.mkdir(exist_ok=True)

    min_sample_size = 30
    archetype_columns = [
        ["small_area", "period_built"],
        ["cso_ed_id", "period_built"],
        ["countyname", "period_built"],
        ["period_built"],
    ]
    for archetype in archetype_columns:
        archetype_name = "_".join(archetype)
        sample_size_column = f"sample_size__{archetype_name}"
        archetype_group_sizes = (
            buildings.groupby(archetype).size().rename(sample_size_column)
        )
        agg_columns = set(buildings.columns).difference(set(archetype))
        aggregation_operations = _get_aggregation_operations(buildings[agg_columns])
        agg_buildings = buildings.groupby(archetype).agg(aggregation_operations)
        agg_buildings_of_sufficient_size = (
            agg_buildings.join(archetype_group_sizes)
            .query(f"`{sample_size_column}` > @min_sample_size")
            .reset_index()
        )
        agg_buildings_of_sufficient_size["archetype"] = archetype_name
        agg_buildings_of_sufficient_size.to_csv(
            dirpath / f"{archetype_name}.csv", index=False
        )


def fill_unknown_buildings_with_archetypes(product: Any, upstream: Any) -> None:
    buildings = pd.read_parquet(upstream["fill_census_with_bers"])
    input_dirpath = Path(upstream["create_archetypes"])
    output_dirpath = Path(product)
    output_dirpath.mkdir(exist_ok=True)

    unknown_buildings = buildings[buildings["countyname"].isnull()]
    archetype_groups = [
        ["small_area", "period_built"],
        ["cso_ed_id", "period_built"],
        ["countyname", "period_built"],
        ["period_built"],
    ]
    for archetype_columns in archetype_groups:
        unknown_buildings = unknown_buildings[unknown_buildings["countyname"].isnull()]
        filename = "_".join(archetype_columns) + ".csv"
        archetypes = pd.read_csv(input_dirpath / filename)
        unknown_buildings = (
            unknown_buildings.set_index(archetype_columns)
            .combine_first(archetypes.set_index(archetype_columns))
            .reset_index()
        )
        unknown_buildings.dropna(subset=["countyname"]).to_csv(output_dirpath / filename)


def combine_known_and_archetyped_buildings(product: Any, upstream: Any) -> None:
    buildings = pd.read_parquet(upstream["fill_census_with_bers"])
    known_buildings = buildings[buildings["countyname"].notnull()]
    unknown_buildings = [
        pd.read_csv(f)
        for f in Path(upstream["fill_unknown_buildings_with_archetypes"]).iterdir()
    ]
    estimated_buildings = pd.concat([known_buildings] + unknown_buildings)
    estimated_buildings.to_csv(product, index=False)
