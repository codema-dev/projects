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
# # Read Dublin Census Small Area Stock
use_columns = ["EDNAME", "SMALL_AREA", "period_built", "BERBand"]
ber_private_dublin = pd.read_csv(data_dir / "BER_Dublin.09.06.2020.csv")[
    use_columns
].assign(
    category_id=lambda df: df.groupby(["EDNAME", "SMALL_AREA", "period_built"])
    .cumcount()
    .apply(lambda x: x + 1),  # add 1 to each row as cumcount starts at 0
)

# %% [markdown]
# # Read Dublin Census Small Area Stock
dublin_indiv_hh_bers = (
    pd.read_csv(data_dir / "dublin_small_area_hh_age_indiv.csv")
    .assign(
        category_id=lambda df: df.groupby(["EDNAME", "SMALL_AREA", "period_built"])
        .cumcount()
        .apply(lambda x: x + 1),  # add 1 to each row as cumcount starts at 0
    )
    .merge(
        ber_private_dublin.drop(columns="EDNAME"),
        how="left",
    )  # link each building to its corresponding BER if exists
)


# %% [markdown]
# # Read Amalgamate individual buildings to Small Areas

# %%
estimated_dublin_small_area_bers = (
    dublin_indiv_hh_bers.pivot_table(
        index=["EDNAME", "SMALL_AREA"],
        columns="estimated_ber",
        values="period_built",
        aggfunc="count",
    )
    .fillna(0)
    .reset_index(drop=True)
    .assign(
        total=lambda df: df.eval("A + C + D + E"),
        A_to_B=lambda df: df["A"],
        C_to_D=lambda df: df.eval("C + D"),
        E_to_G=lambda df: df["E"],
    )
    .rename(columns=lambda name: name + "_estimate")
    .rename(columns={"total_estimate": "census_total"})
    .join(dublin_small_area_boundaries)
    .pipe(gpd.GeoDataFrame)
)

# %%
sample_dublin_small_area_bers = (
    dublin_indiv_hh_bers.pivot_table(
        index=["EDNAME", "SMALL_AREA"],
        columns="BERBand",
        values="period_built",
        aggfunc="count",
    )
    .fillna(0)
    .reset_index(drop=True)
    .assign(
        total=lambda df: df.eval("A + B + C + D + E + F + G"),
        A_to_B=lambda df: df.eval("A + B"),
        C_to_D=lambda df: df.eval("C + D"),
        E_to_G=lambda df: df.eval("E + F + G"),
    )
    .rename(columns=lambda name: name + "_sample")
    .join(dublin_small_area_boundaries)
    .pipe(gpd.GeoDataFrame)
)

# %%
dublin_small_area_bers = pd.concat(
    [
        estimated_dublin_small_area_bers,
        sample_dublin_small_area_bers.drop(
            columns=["EDNAME", "SMALL_AREA", "geometry"]
        ),
    ],
    axis=1,
)

# %% [markdown]
# # Plot Small Area BERs

# %%
pandas_bokeh.output_file(html_dir / "estimated_dublin_small_area_bers.html")

# %%
hovertool_string = """
    <h2>@EDNAME<br>@SMALL_AREA</h2>
    
    <table style="background-color:#084594;color:#ffffff">
        <tr>
            <th>BER<br>Rating</th>
            <th>Estimated<br>BER<br>Rating</th>
            <th>Sample<br>BER<br>Rating</th>
        </tr>
        <tr>
            <td>A</td>
            <td>@A_estimate</td>
            <td>@A_sample</td>
        </tr>
        <tr>
            <td>B</td>
            <td>-</td>
            <td>@B_sample</td>
        </tr>
        <tr>
            <td>C</td>
            <td>@C_estimate</td>
            <td>@C_sample</td>
        </tr>
        <tr>
            <td>D</td>
            <td>@D_estimate</td>
            <td>@D_sample</td>
        </tr>
        <tr>
            <td>E</td>
            <td>@E_estimate</td>
            <td>@E_sample</td>
        </tr>
        <tr>
            <td>F</td>
            <td>-</td>
            <td>@F_sample</td>
        </tr>
        <tr>
            <td>G</td>
            <td>-</td>
            <td>@G_sample</td>
        </tr>
        <tr>
            <td><strong>Total</strong></td>
            <td><strong>@census_total</strong></td>
            <td><strong>@total_sample</strong></td>
        </tr>
    </table>
"""
dublin_small_area_bers.plot_bokeh(
    figsize=(700, 900),
    simplify_shapes=50,
    dropdown=["E_to_G_estimate", "C_to_D_estimate", "A_to_B_estimate"],
    colormap=(
        "#f7fbff",
        "#084594",
    ),
    colormap_range=(0, 80),
    hovertool_string=hovertool_string,
)

# %%
