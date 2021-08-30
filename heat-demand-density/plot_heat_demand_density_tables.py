from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import dataframe_image as dfi

import pandas_bokeh
from globals import DATA_DIR

pandas_bokeh.output_notebook()
pd.set_option("display.precision", 1)

## Parametrize
# overwrite parameters with arguemnts generated in prefect pipeline

# + tags=["parameters"]
SAVE_AS_HTML: bool = True
SAVE_AS_IMAGE: bool = True
DATA_DIR: Path = Path(DATA_DIR)
hdd_map_filepath: Path = (
    DATA_DIR / "processed" / "dublin_small_area_demand_tj_per_km2.geojson"
)
# -

## Load

hdd_map = gpd.read_file(hdd_map_filepath)

## Set Globals

local_authorities = hdd_map["local_authority"].unique()

## Calculate Totals

hdd_map["total_heat_demand_tj_per_km2y"] = (
    hdd_map["residential_heat_demand_tj_per_km2y"]
    + hdd_map["non_residential_heat_demand_tj_per_km2y"]
)

hdd_map["total_heat_demand_mwh_per_y"] = (
    hdd_map["residential_heat_demand_tj_per_km2y"]
    + hdd_map["non_residential_heat_demand_tj_per_km2y"]
)

## Categorise demands

hdd_map["feasibility"] = pd.cut(
    hdd_map["total_heat_demand_tj_per_km2y"],
    bins=[-np.inf, 20, 50, 120, 300, np.inf],
    labels=[
        "Not Feasible",
        "Future Potential",
        "Feasible with Supporting Regulation",
        "Feasible",
        "Very Feasible",
    ],
)
hdd_map["category"] = hdd_map["feasibility"].cat.codes

## Amalgamate Demands to Local Authority Level

hdd_map_table = (
    hdd_map.drop(
        columns=[
            "small_area",
            "category",
            "geometry",
        ]
    )
    .groupby(["local_authority", "feasibility"])
    .sum()
    .round()
    .reset_index()
)
hdd_map_table["band"] = hdd_map_table["feasibility"].map(
    {
        "Not Feasible": "<20",
        "Future Potential": "20-50",
        "Feasible with Supporting Regulation": "50-120",
        "Feasible": "120-300",
        "Very Feasible": ">300",
    }
)

## Plot Demand Map & Glossary

for la in local_authorities:

    la_table = (
        hdd_map_table.query("local_authority == @la")
        .copy()
        .reset_index(drop=True)
        .drop(columns="local_authority")
    )
    total_heat = la_table["total_heat_demand_mwh_per_y"].sum()
    la_table["percentage_share_of_heat_demand"] = (
        la_table["total_heat_demand_mwh_per_y"]
        .divide(total_heat)
        .multiply(100)
        .round(1)
    )

    ## Style each table row with it's corresponding color
    idx = pd.IndexSlice
    styled_table = (
        la_table.astype(
            {
                "residential_heat_demand_tj_per_km2y": "int32",
                "non_residential_heat_demand_tj_per_km2y": "int32",
                "total_heat_demand_tj_per_km2y": "int32",
                "number_of_residential_buildings": "int32",
                "number_of_non_residential_buildings": "int32",
                "polygon_area_km2": "int32",
            }
        )
        .rename(
            columns={
                "feasibility": "Feasibility",
                "residential_heat_demand_tj_per_km2y": "Residential [TJ/km²year]",
                "non_residential_heat_demand_tj_per_km2y": "Non-Residential [TJ/km²year]",
                "total_heat_demand_tj_per_km2y": "Total [TJ/km²year]",
                "band": "Band [TJ/km²year]",
                "percentage_share_of_heat_demand": "% Share [MWh/year]",
                "number_of_residential_buildings": "Residential Buildings",
                "number_of_non_residential_buildings": "Non-Residential Buildings",
                "polygon_area_km2": "Area [km²]",
            }
        )
        .drop(
            columns=[
                "residential_heat_demand_mwh_per_y",
                "non_residential_heat_demand_mwh_per_y",
                "total_heat_demand_mwh_per_y",
            ]
        )
        .set_index("Feasibility")
        .style
    )
    feasibilities = [
        "Not Feasible",
        "Future Potential",
        "Feasible with Supporting Regulation",
        "Feasible",
        "Very Feasible",
    ]
    opacity = 0.75
    colors = [
        f"rgba(255,255,178,{opacity})",
        f"rgba(254,204,92,{opacity})",
        f"rgba(253,141,60,{opacity})",
        f"rgba(240,59,32,{opacity})",
        f"rgba(189,0,38,{opacity})",
    ]
    for feasibility, color in zip(feasibilities, colors):
        styled_table = styled_table.set_properties(
            **{"background": color},
            axis=1,
            subset=idx[idx[feasibility], idx[:]],
        )

    filename = la.replace(" ", "-").replace("ú", "u")
    if SAVE_AS_HTML:
        with open(
            Path(DATA_DIR) / "maps" / f"Glossary-{filename}.html", "w", encoding="utf-8"
        ) as file:
            styled_table.to_html(file)

    if SAVE_AS_IMAGE:
        dfi.export(
            styled_table, str(Path(DATA_DIR) / "maps" / f"Glossary-{filename}.png")
        )
