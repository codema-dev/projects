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

# %% [markdown]
# # Read EPA Industrial Energy Demands
kwh_columns = [
    "Diesel Use [kWh/y]",
    "Gas Oil [kWh/y]",
    "Light Fuel Oil Use [kWh/y]",
    "Heavy Fuel Oil Use [kWh/y]",
    "Natural Gas Use [kWh/y]",
    "Electricity Generated [kWh/y]",
    "Renewable Electricity Generated [kWh/y]",
]
epa_industrial_sites = pd.read_excel(data_dir / "epa_industrial_sites.xlsx")

# %% [markdown]
# # Read VO Private Anonymised
# ... building IDs with empty floor areas in vo public have been anonymised in VO private
vo_private_anonymised = (
    gpd.read_file(data_dir / "vo_private_anonymised.geojson", driver="GeoJSON")
    .query("Benchmark not in ['None', 'Unknown']")
    .assign(
        Use=lambda gdf: gdf["Property Use"].str.capitalize(),
        typical_ff=lambda gdf: gdf["typical_ff"]
        .replace({0: np.nan})
        .astype(str)
        .replace({"nan": ""}),
        industrial_sh=lambda gdf: gdf["industrial_sh"]
        .replace({0: np.nan})
        .astype(str)
        .replace({"nan": ""}),
        ID=lambda df: df["ID"].astype("int32"),
    )
)

# %%
columns = [
    "Benchmark",
    "Use",
    "inferred_area_m2",
    "heating_mwh_per_year",
    "area_is_estimated",
    "latitude",
    "longitude",
]
vo_private_anonymised_aggregated = (
    vo_private_anonymised.copy()
    .loc[:, columns]
    .apply(lambda x: x.astype(str), axis="columns")
    .groupby(["latitude", "longitude"])
    .transform(lambda x: "".join("<td>" + x + "</td>\n"))
    .join(vo_private_anonymised, lsuffix="_agg")
)  # add new HTML columns for locations with multiple properties

# %%
vo_private_anonymised_reduced = (
    vo_private_anonymised_aggregated.groupby(["latitude", "longitude"])[
        "inferred_area_m2"
    ]
    .transform("sum")
    .to_frame()
    .join(vo_private_anonymised_aggregated, lsuffix="_summed")
    .drop_duplicates(subset=["latitude", "longitude"])
    .pipe(gpd.GeoDataFrame)
)  # sum locations with duplicate properties to find >1000m² & drop duplicate locations

# %% [markdown]
# # Plot

# %%
industrial = vo_private_anonymised_reduced.query("Industrial == 1")

# %%
uses = [
    "CENTRE FOR ASYLUM SEEKERS",
    "COLLEGE",
    "LIBRARY",
    "MUSEUM",
    "MUSEUM HERITAGE / INTERPRETATIVE CENTRE",
    "SCHOOL",
]
benchmarks = [
    "Hospital (clinical and research)",
    "Emergency services",
]
public_sector = vo_private_anonymised_reduced.query(
    "Benchmark in @benchmarks | `Property Use` in @uses"
)

# %%
less_than_1000_m2 = vo_private_anonymised_reduced.query(
    "inferred_area_m2_summed < 1000"
    "& Use not in @industrial.Use"
    "& Use not in @public_sector.Use"
)

# %%
greater_than_1000_m2 = vo_private_anonymised_reduced.query(
    "inferred_area_m2_summed > 1000"
    "& Use not in @industrial.Use"
    "& Use not in @public_sector.Use"
)

# %%
pandas_bokeh.output_file(html_dir / "valuation_office_buildings_2015.html")

# %%
hovertool_string = """
<table border="1", style="background-color:{};text-align:center">
    <tr>
        <th>Benchmark</th>
        @Benchmark_agg
    </tr>
    <tr>
        <th>Use</th>
        @Use_agg
    </tr>
    <tr>
        <th>Area<br>[m²]</th>
        @inferred_area_m2_agg
    </tr>
    <tr>
        <th>Estimated Annual<br>Heat Demand<br>[MWh/m²y]</th>
        @heating_mwh_per_year_agg
    </tr>
    <tr>
        <th>Area is<br>estimated?</th>
        @area_is_estimated_agg
    </tr>
</table>
"""

figure = less_than_1000_m2.plot_bokeh(
    figsize=(700, 900),
    hovertool_string=hovertool_string.format("#ffeda0"),
    color="#ffeda0",
    legend="<1000m²",
)

greater_than_1000_m2.plot_bokeh(
    figure=figure,
    hovertool_string=hovertool_string.format("#feb24c"),
    color="#feb24c",
    legend=">1000m²",
)

industrial.plot_bokeh(
    figure=figure,
    hovertool_string=hovertool_string.format("#f03b20"),
    color="#f03b20",
    legend="Industrial",
)

public_sector.plot_bokeh(
    figure=figure,
    hovertool_string=hovertool_string.format("#2c7fb8"),
    color="#2c7fb8",
    legend="Public Sector",
)


# %%
