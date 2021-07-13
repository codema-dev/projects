# %%
from pathlib import Path
import re

from bokeh.palettes import diverging_palette
import geopandas as gpd
import pandas as pd

import pandas_bokeh

from dublin_building_stock.spatial_operations import get_geometries_within

data_dir = Path("../data")
html_dir = Path("../html")
pandas_bokeh.output_notebook()

# %% [markdown]
# # Read Small Area Boundaries
dublin_small_area_boundaries_2016 = gpd.read_file(
    data_dir
    / "Dublin_Small_Areas_Ungeneralised_-_OSi_National_Statistical_Boundaries_-_2015-shp"
)


# %% [markdown]
# # Read Municipality Boundaries
dublin_municipality_boundaries = gpd.read_file(
    data_dir / "Dublin_Census2011_Admin_Counties_generalised20m"
).rename(columns={"COUNTYNAME": "local_authority"})


# %% [markdown]
# # Read Dublin Census Small Area Stock
dublin_small_area_boilers = (
    pd.read_csv(data_dir / "dublin_small_area_hh_boilers.csv")
    .merge(dublin_small_area_boundaries_2016)
    .rename(columns=lambda name: re.sub(r"[ ()]", "_", name.lower()).replace(".", ""))
    .pipe(gpd.GeoDataFrame)
    .pipe(get_geometries_within, dublin_municipality_boundaries.to_crs(epsg=2157))
)

# %%
drop_columns = ["small_area", "total", "edname", "geometry"]
dublin_small_area_boiler_totals = (
    dublin_small_area_boilers.drop(columns=drop_columns)
    .groupby("local_authority")
    .sum()
    .reset_index()
    .rename(columns={"index": "Boiler", 0: "Total"})
    .set_index("local_authority")
    .T.assign(Total=lambda df: df.sum(axis="columns"))
    .rename(columns={"Total": "All of Dublin"})
    .rename_axis(index="Boiler")
    .pipe(pd.DataFrame)
    # .melt(id_vars="local_authority", var_name="boiler", value_name="total")
)

# %% [markdown]
# # Plot Small Area Boiler Breakdown Map

# %%
pandas_bokeh.output_file(html_dir / "dublin_small_area_boilers.html")

# %%
hovertool_string = """
    <h2>@edname<br>@small_area</h2>
    
    <table style="background-color:#084594;color:#ffffff">
        <tr>
            <th>Boiler<br>Type</th>
            <th>Number of Dwellings</th>
        </tr>
        <tr>
            <td>No Central Heating</td>
            <td>@no_central_heating</td>
        </tr>
        <tr>
            <td>Natural gas</td>
            <td>@natural_gas</td>
        </tr>
        <tr>
            <td>Electricity</td>
            <td>@electricity</td>
        </tr>
        <tr>
            <td>Oil</td>
            <td>@oil</td>
        </tr>
        <tr>
            <td>Coal</td>
            <td>@coal__incl_anthracite_</td>
        </tr>
        <tr>
            <td>Peat</td>
            <td>@peat__incl_turf_</td>
        </tr>
        <tr>
            <td>LPG</td>
            <td>@liquid_petroleum_gas__lpg_</td>
        </tr>
        <tr>
            <td>Wood</td>
            <td>@wood__incl_wood_pellets_</td>
        </tr>
        <tr>
            <td>Other</td>
            <td>@other</td>
        </tr>
        <tr>
            <td>Not Stated</td>
            <td>@not_stated</td>
        </tr>
        <tr>
            <td><strong>Total</strong></td>
            <td><strong>@total</strong></td>
        </tr>
    </table>
"""
dublin_small_area_boilers.plot_bokeh(
    figsize=(700, 900),
    dropdown=[
        "natural_gas",
        "electricity",
        "oil",
        "no_central_heating",
        "coal__incl_anthracite_",
    ],
    colormap=(
        "#f7fbff",
        "#084594",
    ),
    colormap_range=(0, 80),
    hovertool_string=hovertool_string,
    fill_alpha=0.5,
)


# %% [markdown]
# # Plot Small Area Boiler Breakdown Bar Chart

# %%
pandas_bokeh.output_file(html_dir / "dublin_boiler_totals.html")

# %%
dublin_small_area_boiler_totals.plot_bokeh.barh()

# %%
