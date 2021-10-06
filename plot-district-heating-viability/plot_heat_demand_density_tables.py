from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

pd.set_option("display.precision", 1)

## Parametrize
# overwrite parameters with arguemnts generated in prefect pipeline

# + tags=["parameters"]
upstream = ["estimate_heat_demand_density", "download_dublin_small_area_boundaries"]
product = None
# -

Path(product["table_dir"]).mkdir(exist_ok=True)

## Load

demand = pd.read_csv(upstream["estimate_heat_demand_density"]["demand"])

density = pd.read_csv(upstream["estimate_heat_demand_density"]["density"])

boundaries = gpd.read_file(upstream["download_dublin_small_area_boundaries"])

## Set Globals

local_authorities = boundaries["local_authority"].unique()

## Create Map

density_map = boundaries.merge(density).merge(demand)

## Calculate Totals

columns = [c for c in density_map.columns if "tj_per_km2" in c]
density_map["total_heat_tj_per_km2"] = density_map[columns].sum(axis=1)

columns = [c for c in density_map.columns if "mwh" in c]
density_map["total_heat_mwh"] = density_map[columns].sum(axis=1)

## Categorise demands

density_map["feasibility"] = pd.cut(
    density_map["total_heat_tj_per_km2"],
    bins=[-np.inf, 20, 50, 120, 300, np.inf],
    labels=[
        "Not Feasible",
        "Future Potential",
        "Feasible with Supporting Regulation",
        "Feasible",
        "Very Feasible",
    ],
)

density_map["category"] = density_map["feasibility"].cat.codes

## Amalgamate Demands to Local Authority Level

use_columns = [
    "local_authority",
    "residential_heat_mwh",
    "commercial_heat_mwh",
    "industrial_heat_mwh",
    "total_heat_mwh",
    "feasibility",
]
density_map_table = (
    density_map.loc[:, use_columns]
    .groupby(["local_authority", "feasibility"])
    .sum()
    .round()
    .reset_index()
)

density_map_table["band"] = density_map_table["feasibility"].map(
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
        density_map_table.query("local_authority == @la")
        .copy()
        .reset_index(drop=True)
        .drop(columns="local_authority")
    )

    total_heat = la_table["total_heat_mwh"].sum()

    la_table["percentage_share_of_heat_demand"] = (
        la_table["total_heat_mwh"].divide(total_heat).multiply(100).round(1)
    )

    ## Style each table row with it's corresponding color
    idx = pd.IndexSlice
    styled_table = (
        la_table.astype(
            {
                "residential_heat_mwh": "int32",
                "commercial_heat_mwh": "int32",
                "industrial_heat_mwh": "int32",
                "total_heat_mwh": "int32",
            }
        )
        .rename(
            columns={
                "feasibility": "Feasibility",
                "residential_heat_mwh": "Residential [MWh/year]",
                "commercial_heat_mwh": "Commercial [MWh/year]",
                "industrial_heat_mwh": "Industrial [MWh/year]",
                "total_heat_mwh": "Total [MWh/year]",
                "band": "Band [TJ/km²year]",
                "percentage_share_of_heat_demand": "% Share [MWh/year]",
            }
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
    with open(
        Path(product["table_dir"]) / f"Glossary-{filename}.html",
        "w",
        encoding="utf-8",
    ) as file:
        styled_table.to_html(file)
