from datetime import datetime
from pathlib import Path

from bokeh.io import save
from bokeh.io import show
from bokeh.models import ColumnDataSource
from bokeh.models import DataTable
from bokeh.models import TableColumn
import geopandas as gpd
import numpy as np
import pandas as pd
import pandas_bokeh

from globals import DATA_DIR

## Parametrize
# overwrite parameters with arguemnts generated in prefect pipeline

# + tags=["parameters"]
SAVE_PLOTS: bool = True
DATA_DIR: str = DATA_DIR
demand_map_filepath: str = (
    DATA_DIR / "processed" / "dublin_small_area_demand_tj_per_km2.geojson"
)
# -

## Load

demand_map = gpd.read_file(demand_map_filepath)

## Set Globals

local_authorities = demand_map["local_authority"].unique()
date = datetime.today().strftime(r"%Y-%m-%d")

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
        "Not Feasible": "<20 TJ/km²year",
        "Future Potential": "20-50 TJ/km²year",
        "Feasible with Supporting Regulation": "50-120 TJ/km²year",
        "Feasible": "120-300 TJ/km²year",
        "Very Feasible": ">300 TJ/km²year",
    }
)

## Split Map & Tables by Local Authority

la_maps = []
la_tables = []
for la in local_authorities:

    la_map = demand_map.query("local_authority == @la")
    la_maps.append(la_map.drop(columns="local_authority"))

    la_table = demand_table.query("local_authority == @la")
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

    Columns = [TableColumn(field=Ci, title=Ci) for Ci in la_table.columns]
    data_table = DataTable(
        source=ColumnDataSource(la_table),
        columns=Columns,
        width=600,
        height=280,
    )

    if SAVE_PLOTS:
        filename = str(date) + " " + la + " Heat Demand Density Table.html"
        save(data_table, filename=Path(DATA_DIR) / "maps" / filename)
    else:
        show(data_table)

    hovertool_string = """
    <h4>@feasibility</h4>
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
    figure = la_map.plot_bokeh(
        figsize=(700, 900),
        category="category",
        colormap=["#ffffb2", "#fecc5c", "#fd8d3c", "#f03b20", "#bd0026"],
        show_colorbar=False,
        fill_alpha=0.5,
        hovertool_string=hovertool_string,
    )
    if SAVE_PLOTS:
        filename = str(date) + " " + la + " Heat Demand Density Map.html"
        save(figure, filename=Path(DATA_DIR) / "maps" / filename)
    else:
        show(figure)
