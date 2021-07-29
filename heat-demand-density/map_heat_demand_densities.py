from pathlib import Path

from bokeh.io import export_svg
from bokeh.io import save
from bokeh.io import show
import geopandas as gpd
import numpy as np
import pandas as pd
import pandas_bokeh

from globals import DATA_DIR

## Parametrize
# overwrite parameters with arguemnts generated in prefect pipeline

# + tags=["parameters"]
SAVE_AS_HTML: bool = True
DATA_DIR: str = DATA_DIR
demand_map_filepath: str = (
    DATA_DIR / "processed" / "dublin_small_area_demand_tj_per_km2.geojson"
)
# -

## Load

demand_map = gpd.read_file(demand_map_filepath)

## Set Globals

local_authorities = demand_map["local_authority"].unique()

## Categorise demands

demand_map["feasibility"] = pd.cut(
    demand_map["total_heat_demand_tj_per_km2y"],
    bins=[-np.inf, 20, 50, 120, 300, np.inf],
    labels=[
        "Not Feasible",
        "Future Potential",
        "Feasible with Supporting Regulation",
        "Feasible",
        "Very Feasible",
    ],
)
demand_map["category"] = demand_map["feasibility"].cat.codes

## Amalgamate Demands to Local Authority Level

demand_table = (
    demand_map.drop(columns=["small_area", "category", "geometry"])
    .groupby(["local_authority", "feasibility"])
    .sum()
    .round()
    .reset_index()
)
demand_table["band"] = demand_table["feasibility"].map(
    {
        "Not Feasible": "<20",
        "Future Potential": "20-50",
        "Feasible with Supporting Regulation": "50-120",
        "Feasible": "120-300",
        "Very Feasible": ">300",
    }
)

## Split Map & Tables by Local Authority

la_maps = []
la_tables = []
for la in local_authorities:

    la_map = demand_map.query("local_authority == @la").reset_index(drop=True)
    la_maps.append(la_map.drop(columns="local_authority"))

    la_table = (
        demand_table.query("local_authority == @la").copy().reset_index(drop=True)
    )
    total_heat = la_table["total_heat_demand_tj_per_km2y"].sum()
    la_table["percentage_share_of_heat_demand"] = (
        la_table["total_heat_demand_tj_per_km2y"]
        .divide(total_heat)
        .multiply(100)
        .round(1)
    )
    la_tables.append(la_table.drop(columns="local_authority"))


## Plot Demand Map & Glossary

for la, la_map, la_table in zip(local_authorities, la_maps, la_tables):

    hovertool_string = """
    <h4>@feasibility</h4>
    <table>
        <t>
            <th>Category</th>
            <th>TJ/km²year</th>
        </tr>
        <tr>
            <td>Total</td>
            <td>@total_heat_demand_tj_per_km2y</td>
        <tr>
        <tr>
            <td>Residential</td>
            <td>@residential_heat_demand_tj_per_km2y</td>
        <tr>
        <tr>
            <td>Non-Residential</td>
            <td>@non_residential_heat_demand_tj_per_km2y</td>
        <tr>
    </table>
    """
    opacity = 0.75
    colors = [
        f"rgba(255,255,178,{opacity})",
        f"rgba(254,204,92,{opacity})",
        f"rgba(253,141,60,{opacity})",
        f"rgba(240,59,32,{opacity})",
        f"rgba(189,0,38,{opacity})",
    ]
    figure = la_map.plot_bokeh(
        figsize=(700, 900),
        category="category",
        colormap=colors,
        show_colorbar=False,
        hovertool_string=hovertool_string,
        legend="Heat Demand Density",
        show_figure=False,
    )

    ## Style each table row with it's corresponding color
    idx = pd.IndexSlice
    styled_table = (
        la_table.astype(
            {
                "residential_heat_demand_tj_per_km2y": "int32",
                "non_residential_heat_demand_tj_per_km2y": "int32",
                "total_heat_demand_tj_per_km2y": "int32",
                "percentage_share_of_heat_demand": "int8",
            }
        )
        .rename(
            columns={
                "feasibility": "Feasibility",
                "residential_heat_demand_tj_per_km2y": "Residential [TJ/km²year]",
                "non_residential_heat_demand_tj_per_km2y": "Non-Residential [TJ/km²year]",
                "total_heat_demand_tj_per_km2y": "Total [TJ/km²year]",
                "band": "Band [TJ/km²year]",
                "percentage_share_of_heat_demand": "% Share",
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
    for feasibility, color in zip(feasibilities, colors):
        styled_table = styled_table.set_properties(
            **{"background": color},
            axis=1,
            subset=idx[idx[feasibility], idx[:]],
        )

    filename = la.replace(" ", "-").replace("ú", "u")
    if SAVE_AS_HTML:
        with open(
            Path(DATA_DIR) / "tables" / f"{filename}.html", "w", encoding="utf-8"
        ) as file:
            styled_table.to_html(file)
        save(figure, filename=Path(DATA_DIR) / "maps" / f"{filename}.html")
    else:
        print(la_table)
        show(figure)
