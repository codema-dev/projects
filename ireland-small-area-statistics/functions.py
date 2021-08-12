import json
from pathlib import Path
import re
from shutil import unpack_archive
from typing import Any
from typing import Dict
from urllib.request import urlretrieve

import numpy as np
import geopandas as gpd
import pandas as pd


def download(url: str, filename: str) -> None:
    if not Path(filename).exists():
        urlretrieve(url=url, filename=filename)


def unzip(filename: str) -> None:
    unzipped_filepath = Path(filename).with_suffix("")
    if not Path(unzipped_filepath).exists():
        unpack_archive(filename=filename, extract_dir=unzipped_filepath)


def read_json(filename: str) -> Dict[str, Any]:
    with open(filename, "r") as f:
        return json.load(f)


def _repeat_rows_on_column(df, on):
    return df.reindex(df.index.repeat(df[on])).drop(columns=on)


def extract_period_built_statistics(statistics: pd.DataFrame) -> pd.DataFrame:
    columns_to_extract = [
        x for x in statistics.columns if re.match(r"T6_2_.*H", x) or x == "GEOGID"
    ]
    return statistics.copy().loc[:, columns_to_extract]


def melt_small_area_statistics_to_individual_buildings(
    statistics: pd.DataFrame,
) -> pd.DataFrame:
    """Wrangle the stock to individual building level.

    Before:
        GEOGID              T6_2_PRE19H     ...
        SA2017_017001001    19              ...

    After:
        small_area          period_built
        017001001           PRE19H
        017001001           PRE19H

    Args:
        sa_stats_raw (pd.DataFrame): overview of buildings

    Returns:
        pd.DataFrame: individual buildings
    """
    return (
        statistics.copy()
        .assign(small_area=lambda df: df["GEOGID"].str[7:])
        .drop(columns="GEOGID")
        .set_index("small_area")
        .rename(columns=lambda x: re.findall(f"T6_2_(.*)H", x)[0])
        .reset_index()
        .melt(id_vars="small_area", var_name="period_built", value_name="total")
        .query("period_built != 'T'")
        .pipe(_repeat_rows_on_column, on="total")
    )


def replace_not_stated_period_built_with_mode(stock: pd.DataFrame) -> pd.Series:
    modal_period_built = (
        stock.assign(period_built=lambda df: df["period_built"].replace({"NS": np.nan}))
        .groupby("small_area")["period_built"]
        .transform(lambda s: s.mode()[0])
    )
    return stock.assign(
        period_built=lambda df: df["period_built"]
        .replace({"NS": np.nan})
        .fillna(modal_period_built)
    )


def map_routing_keys_to_countyname(
    routing_key_boundaries: gpd.GeoDataFrame, counties: Dict[str, str]
) -> pd.DataFrame:
    return routing_key_boundaries.assign(
        countyname=lambda df: df["Descriptor"].map(counties)
    )


def _fill_unknown_countyname(gdf):
    # these small areas are islands and so fall outside the routing key boundaries!
    c = "countyname"
    gdf.loc["077149001/077149002", c] = "CO. KERRY"
    gdf.loc["057133003", c] = "CO. WEXFORD"
    gdf.loc["247076001/247076002", c] = "CO. DONEGAL"
    gdf.loc["057081001/057081008", c] = "CO. DONEGAL"
    return gdf


def _replace_incorrectly_matched_small_areas(gdf):
    # these small areas were placed in the wrong local countyname due to a discrepency
    # between openaddress boundaries & cso boundaries
    c = "countyname"
    co_dublin_sas = [
        "267091001",
        "267120004",
        "267120005",
        "267120006",
        "267120007",
        "267120008",
        "267120009",
        "267122001",
        "267122003",
        "267122016",
        "267122017",
    ]
    gdf.loc[co_dublin_sas, c] = ["CO. DUBLIN"] * len(co_dublin_sas)
    gdf.loc["257046001", c] = "CO. WICKLOW"
    gdf.loc["087071028", c] = "CO. KILDARE"
    gdf.loc["087002003", c] = "CO. KILDARE"
    meath_sas = [
        "167025001/03",
        "167003002",
        "167085001",
        "167085003",
        "167085013",
        "167074003",
        "167003003",
        "167085002",
        "167025001/01",
        "167085014",
        "167003001",
        "167085011",
        "167029005/04",
        "167029005/02",
        "167029005/05",
        "167085005",
        "167085012",
        "167085004",
        "167085006",
        "167085008",
        "167085007",
        "167029005/03",
        "167085009",
        "167085010",
    ]
    gdf.loc[meath_sas, c] = ["CO. MEATH"] * len(meath_sas)
    return gdf


def _replace_erroneous_data(gdf):
    gdf = gdf.set_index("small_area")
    with_known_countyname = _fill_unknown_countyname(gdf)
    return _replace_incorrectly_matched_small_areas(with_known_countyname).reset_index()


def link_small_areas_to_routing_keys(
    small_area_boundaries: gpd.GeoDataFrame, routing_key_boundaries: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    representative_points = small_area_boundaries.assign(
        geometry=lambda gdf: gdf.to_crs(epsg=2157).geometry.representative_point(),
    )[["SMALL_AREA", "CSOED", "COUNTYNAME", "geometry"]].rename(
        columns={
            "SMALL_AREA": "small_area",
            "CSOED": "cso_ed_id",
            "COUNTYNAME": "local_authority",
        }
    )
    small_areas_in_routing_keys = gpd.sjoin(
        representative_points,
        routing_key_boundaries.to_crs(epsg=2157),
        op="within",
        how="left",
    )
    small_areas_in_routing_keys["geometry"] = small_area_boundaries["geometry"]
    return _replace_erroneous_data(
        small_areas_in_routing_keys[
            [
                "small_area",
                "cso_ed_id",
                "countyname",
                "local_authority",
                "RoutingKey",
                "Descriptor",
                "geometry",
            ]
        ]
    )


def extract_dublin(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    return gdf[gdf["countyname"].str.contains("DUBLIN")].copy()


def link_building_ages_to_countyname(
    building_ages: pd.DataFrame, small_area_countynames: gpd.GeoDataFrame
) -> pd.DataFrame:
    return (
        building_ages.merge(small_area_countynames)
        .drop(columns="geometry")
        .pipe(pd.DataFrame)
    )


def to_parquet(df: pd.DataFrame, path: str) -> None:
    df.to_parquet(path)


def to_file(gdf: gpd.GeoDataFrame, path: str, driver: str) -> None:
    gdf.to_file(path, driver=driver)
