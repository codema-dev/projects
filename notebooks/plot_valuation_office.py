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
# # Read HDD
vo_private = gpd.read_file(data_dir / "vo_private.geojson", driver="GeoJSON")

# %% [markdown]
# # Plot

# %%
industrial = vo_private.query("Industrial == 1")

# %%
non_industrial = vo_private.query(
    "Industrial != 1 & Benchmark not in ['None', 'Unknown']"
)

# %%
pandas_bokeh.output_file(html_dir / "valuation_office_buildings_2015.html")

# %%
hovertool_string = """
<table border="1", style="background-color:#084594;color:#ffffff;text-align:center">
    <tr>
        <th>Use</th>
        <td>@Use</td>
    </tr>
    <tr>
        <th>Category</th>
        <td>@Benchmark</td>
    </tr>
    <tr>
        <th>Estimated<br>Annual<br>Heat<br>Demand<br>[MWh/m²y]</th>
        <td>@heating_mwh_per_year</td>
    </tr>
    <tr>
        <th>Benchmark<br>[kWh/m²y]</th>
        <td>@typical_ff@industrial_sh</td>
    </tr>
    <tr>
        <th>Area<br>[m²]</th>
        <td>@inferred_area_m2</td>
    </tr>
    <tr>
        <th>Area<br>is<br>estimated?</th>
        <td>@area_is_estimated</td>
    </tr>
</table>
"""

figure = non_industrial.plot_bokeh(
    figsize=(700, 900),
    fill_alpha=0.5,
    hovertool_string=hovertool_string,
    color="blue",
    legend="Non-Industrial",
)

industrial.plot_bokeh(
    figure=figure,
    fill_alpha=0.5,
    hovertool_string=hovertool_string,
    color="red",
    legend="Industrial",
)


# %%
