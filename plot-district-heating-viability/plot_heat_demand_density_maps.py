from pathlib import Path

from bokeh.io import export_png
from bokeh.io import save
import geopandas as gpd
import numpy as np
import pandas as pd

import pandas_bokeh
from globals import DATA_DIR

pd.set_option("display.precision", 1)

## Parametrize
# overwrite parameters with arguemnts generated in prefect pipeline

# + tags=["parameters"]
SAVE_AS_HTML: bool = False
SAVE_AS_IMAGE: bool = False
DATA_DIR: Path = Path(DATA_DIR)
hdd_map_filepath: Path = (
    DATA_DIR / "processed" / "dublin_small_area_demand_tj_per_km2.gpkg"
)
# -

if not SAVE_AS_HTML:
    pandas_bokeh.output_notebook()

## Load

hdd_map = gpd.read_file(hdd_map_filepath)

## Set Globals

local_authorities = hdd_map["local_authority"].unique()

## Calculate Totals

hdd_map["total_heat_demand_tj_per_km2y"] = (
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


## Plot Demand Map & Glossary

for la in local_authorities:

    la_map = hdd_map.query("local_authority == @la").reset_index(drop=True)

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

    if SAVE_AS_HTML:
        filename = la.replace(" ", "-").replace("ú", "u")
        save(
            obj=figure,
            filename=Path(DATA_DIR) / "maps" / f"Map-{filename}.html",
            title="Heat Demand Density",
        )

    if SAVE_AS_IMAGE:
        export_png(figure, filename=Path(DATA_DIR) / "maps" / f"Map-{filename}.png")
