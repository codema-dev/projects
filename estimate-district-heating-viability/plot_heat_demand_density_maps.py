from pathlib import Path

from bokeh.io import save
import geopandas as gpd
import numpy as np
import pandas as pd

import pandas_bokeh

pandas_bokeh.output_notebook()
pd.set_option("display.precision", 1)

## Parametrize
# overwrite parameters with arguemnts generated in prefect pipeline

# + tags=["parameters"]
upstream = ["estimate_heat_demand_density", "download_dublin_small_area_boundaries"]
product = None
# -

Path(product["map_dir"]).mkdir(exist_ok=True)

## Load

density = pd.read_csv(upstream["estimate_heat_demand_density"]["density"])

boundaries = gpd.read_file(upstream["download_dublin_small_area_boundaries"])

## Set Globals

local_authorities = boundaries["local_authority"].unique()

## Create Map

density_map = boundaries.merge(density)

## Calculate Totals

columns = [c for c in density.columns if "heat" in c]
density_map["total_heat_tj_per_km2"] = density_map[columns].sum(axis=1)

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


## Plot Demand Map & Glossary

for la in local_authorities:

    la_map = density_map.query("local_authority == @la").reset_index(drop=True)

    hovertool_string = """
    <h4>@feasibility</h4>
    <p>Small Area: @small_area</p>

    <table>
        <t>
            <th>Category</th>
            <th>TJ/km²year</th>
        </tr>
        <tr>
            <td>Total</td>
            <td>@total_heat_tj_per_km2</td>
        <tr>
        <tr>
            <td>Residential</td>
            <td>@residential_heat_tj_per_km2</td>
        <tr>
        <tr>
            <td>Commercial</td>
            <td>@commercial_heat_tj_per_km2</td>
        <tr>
        <tr>
            <td>Industrial</td>
            <td>@industrial_heat_tj_per_km2</td>
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

    filename = la.replace(" ", "-").replace("ú", "u")
    save(
        obj=figure,
        filename=Path(product["map_dir"]) / f"Map-{filename}.html",
        title="Heat Demand Density",
    )

density_map.astype({"feasibility": "string"}).to_file(product["gpkg"], driver="GPKG")
