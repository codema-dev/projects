import json
from typing import Any

import geopandas as gpd
import pandas as pd


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
    gdf.loc[["267103003", "267122002"], c] = ["CO. DUBLIN"] * 2
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
    product: Any,
    upstream: Any,
) -> None:
    small_area_boundaries = gpd.read_file(
        str(upstream["download_ireland_small_area_boundaries"])
    ).rename(columns=str.lower).rename(columns={"countyname": "local_authority"})
    routing_key_boundaries = gpd.read_file(
        str(upstream["download_routing_key_boundaries"])
    ).rename(columns=str.lower)
    with open(upstream["download_routing_key_descriptor_to_postcode_map"], "r") as f:
        routing_key_descriptor_map = json.load(f)
    routing_key_boundaries["countyname"] = routing_key_boundaries["descriptor"].map(
        routing_key_descriptor_map
    )

    columns = ["small_area", "csoed", "edname", "local_authority"]
    representative_points = gpd.GeoDataFrame(
        pd.concat(
            [
                small_area_boundaries[columns],
                small_area_boundaries.geometry.representative_point().rename("geometry"),
            ],
            axis=1,
        ),
        crs="EPSG:2157",
    )
    small_areas_in_routing_keys = representative_points.sjoin(
        routing_key_boundaries,
        predicate="within",
        how="left",
    ).drop(columns="index_right").pipe(_replace_erroneous_data)
    
    small_areas_in_routing_keys["geometry"] = small_area_boundaries["geometry"]
    small_areas_in_routing_keys.to_file(str(product), driver="GPKG")
