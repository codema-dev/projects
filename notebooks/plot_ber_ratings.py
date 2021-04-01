# %%
from pathlib import Path

from bokeh.palettes import diverging_palette
import geopandas as gpd
import pandas as pd

import pandas_bokeh

data_dir = Path("../data")
html_dir = Path("../html")
pandas_bokeh.output_notebook()

# %% [markdown]
# # Read Small Area Boundaries
dublin_small_area_boundaries = gpd.read_file(
    data_dir / "dublin_small_area_boundaries.geojson",
    driver="GeoJSON",
)

# %% [markdown]
# # Plot Estimated Small Area BERs

# %%
estimated_dublin_small_area_bers = (
    pd.read_csv(data_dir / "dublin_small_area_hh_age_indiv.csv")
    .pivot_table(
        index=["EDNAME", "SMALL_AREA"],
        columns="estimated_ber",
        values="period_built",
        aggfunc="count",
    )
    .fillna(0)
    .reset_index()
    .assign(
        total=lambda df: df.eval("A + C + D + E"),
        A_to_B=lambda df: df.eval("A"),
        C_to_D=lambda df: df.eval("C + D"),
        E_to_G=lambda df: df.eval("E"),
    )
    .merge(dublin_small_area_boundaries)
    .pipe(gpd.GeoDataFrame)
)

# %%
pandas_bokeh.output_file(html_dir / "estimated_dublin_small_area_bers.html")

# %%
hovertool_string = """
    <h2>  @EDNAME | @SMALL_AREA </h2>
    
    <table style="background-color:#084594;color:#ffffff">
        <tr>
            <th>Estimated BER Rating</th>
            <th>Number of Buildings</th>
        </tr>
        <tr>
            <td>A</td>
            <td>@A</td>
        </tr>
        <tr>
            <td>C</td>
            <td>@C</td>
        </tr>
        <tr>
            <td>D</td>
            <td>@D</td>
        </tr>
        <tr>
            <td>E</td>
            <td>@E</td>
        </tr>
        <tr>
            <td><strong>Total</strong></td>
            <td><strong>@total</strong></td>
        </tr>
    </table>
"""
estimated_dublin_small_area_bers.plot_bokeh(
    figsize=(700, 900),
    simplify_shapes=50,
    dropdown=["E_to_G", "C_to_D", "A_to_B", "A", "C", "D", "E"],
    colormap=(
        "#f7fbff",
        "#084594",
    ),
    colormap_range=(0, 80),
    hovertool_string=hovertool_string,
)

# %% [markdown]
# # Plot Small Area BER Sample

# %%
sample_dublin_small_area_bers = (
    pd.read_csv(data_dir / "BER_Dublin.09.06.2020.csv")
    .pivot_table(
        index=["EDNAME", "SMALL_AREA"],
        columns="BERBand",
        values="CountyName2",
        aggfunc="count",
    )
    .fillna(0)
    .reset_index()
    .assign(
        total=lambda df: df.eval("A + B + C + D + E + F + G"),
        A_to_B=lambda df: df.eval("A + B"),
        C_to_D=lambda df: df.eval("C + D"),
        E_to_G=lambda df: df.eval("E + F + G"),
    )
    .merge(dublin_small_area_boundaries)
    .pipe(gpd.GeoDataFrame)
)

# %%
pandas_bokeh.output_file(html_dir / "sample_dublin_small_area_bers.html")

# %%
hovertool_string = """
    <h2>  @EDNAME | @SMALL_AREA </h2>
    
    <table style="background-color:#084594;color:#ffffff">
        <tr>
            <th>Estimated BER Rating</th>
            <th>Number of Buildings</th>
        </tr>
        <tr>
            <td>A</td>
            <td>@A</td>
        </tr>
        <tr>
            <td>B</td>
            <td>@B</td>
        </tr>
        <tr>
            <td>C</td>
            <td>@C</td>
        </tr>
        <tr>
            <td>D</td>
            <td>@D</td>
        </tr>
        <tr>
            <td>E</td>
            <td>@E</td>
        </tr>
        <tr>
            <td>F</td>
            <td>@F</td>
        </tr>
        <tr>
            <td>G</td>
            <td>@G</td>
        </tr>
        <tr>
            <td><strong>Total</strong></td>
            <td><strong>@total</strong></td>
        </tr>
    </table>
"""
sample_dublin_small_area_bers.plot_bokeh(
    figsize=(700, 900),
    simplify_shapes=50,
    dropdown=["E_to_G", "C_to_D", "A_to_B", "A", "B", "C", "D", "E", "F", "G"],
    colormap=(
        "#f7fbff",
        "#084594",
    ),
    colormap_range=(0, 80),
    hovertool_string=hovertool_string,
)

# %%
