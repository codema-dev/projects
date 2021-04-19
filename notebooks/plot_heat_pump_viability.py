# %%
from pathlib import Path

from bokeh.palettes import diverging_palette
import geopandas as gpd
import numpy as np
import pandas as pd

import pandas_bokeh

data_dir = Path("../data")
html_dir = Path("../html")
pandas_bokeh.output_notebook()

# %%
dublin_small_area_boundaries_2011 = gpd.read_file(
    data_dir / "dublin_small_area_boundaries_2011.geojson", driver="GeoJSON"
)

# %%
heat_pump_ready_sas = (
    pd.read_csv(data_dir / "dublin_indiv_hh.csv", low_memory=False)
    .groupby("SMALL_AREA", as_index=False)["heat_pump_ready"]
    .apply(lambda x: np.round(100 * x.sum() / len(x)))
    .merge(dublin_small_area_boundaries_2011)
    .pipe(gpd.GeoDataFrame)
)

# %%
pandas_bokeh.output_file(html_dir / "heat_pump_ready_hhs.html")

# %%
hovertool_string = """
    <h2>@EDNAME<br>@SMALL_AREA</h2>
    
    <table>
        <tr>
            <th>Heat Pump Ready<br>(estimated)</th>
            <th>@heat_pump_ready %</th>
        </tr>
    </table>
"""
heat_pump_ready_sas.plot_bokeh(
    figsize=(700, 900),
    category="heat_pump_ready",
    hovertool_string=hovertool_string,
)

# %%
