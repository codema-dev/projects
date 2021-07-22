import geopandas as gpd
import numpy as np
import pandas as pd
import pandas_bokeh

from globals import DATA_DIR


pandas_bokeh.output_notebook()
SAVE_PLOTS = True

## Load
small_area_demands = gpd.read_file(
    DATA_DIR / "processed" / "dublin_small_area_demand_tj_per_km2.geojson"
)

## Categorise demands
small_area_demands["feasibility"] = pd.cut(
    small_area_demands["total_heat_demand_tj_per_km2y"],
    bins=[-np.inf, 20, 50, 120, 300, np.inf],
    labels=[
        "Not Feasible<br>[<20 TJ/km²year]",
        "Future Potential<br>[20-50 TJ/km²year]",
        "Feasible with<br>Supporting Regulation<br>[50-120 TJ/km²year]",
        "Feasible<br>[120-300 TJ/km²year]",
        "Very Feasible<br>[>300 TJ/km²year]",
    ],
)
small_area_demands["category"] = small_area_demands["feasibility"].cat.codes


## Plot
for local_authority in small_area_demands["local_authority"].unique():
    hovertool_string = """
    <h3>Heat Demand Density</h3>
    <table>
        <tr>
            <th>Category</th>
            <td>TJ/km²year</td>
        </tr>
        <tr>
            <th>Total</th>
            <td>@total_heat_demand_tj_per_km2y</td>
        <tr>
        <tr>
            <th>Residential</th>
            <td>@residential_heat_demand_tj_per_km2y</td>
        <tr>
        <tr>
            <th>Non-Residential</th>
            <td>@non_residential_heat_demand_tj_per_km2y</td>
        <tr>
    </table>
    """
    if SAVE_PLOTS:
        filename = local_authority + " Heat Demand Density.html"
        pandas_bokeh.output_file(DATA_DIR / "maps" / filename)
    figure = small_area_demands.query("local_authority == @local_authority").plot_bokeh(
        figsize=(700, 900),
        category="category",
        colormap=["#ffffb2", "#fecc5c", "#fd8d3c", "#f03b20", "#bd0026"],
        show_colorbar=False,
        fill_alpha=0.5,
        hovertool_string=hovertool_string,
    )
