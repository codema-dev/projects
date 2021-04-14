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
    data_dir / "Dublin_Census2011_Small_Areas_generalised20m"
)

# %%
valuation_office_private = gpd.read_file(
    data_dir / "valuation_office_private.gpkg", driver="GPKG"
).assign(Use=lambda gdf: gdf["Property Use"].str.capitalize())


# %% [markdown]
# # Estimate Heat Demand Density
# ... using Non-Residential & Residential estimates

# %%
def calculate_hdd_per_year(df, name, dublin_small_area_boundaries_2011):
    return (
        df.groupby("SMALL_AREA")["heating_mwh_per_year"]
        .sum()
        .multiply(10 ** -3)
        .round(2)
        .fillna(0)
        .rename(f"{name}_gwh_per_year")
        .reset_index()
        .merge(dublin_small_area_boundaries_2011[["SMALL_AREA", "geometry"]])
        .pipe(gpd.GeoDataFrame)
        .assign(
            area_km2=lambda gdf: gdf.area * 10 ** -6,
            hdd_tj_per_km2_year=lambda gdf: np.round(
                gdf[f"{name}_gwh_per_year"] * 3.6 / gdf["area_km2"], 2
            ),
        )
        .rename(columns={"hdd_tj_per_km2_year": f"{name}_tj_per_km2_year"})
        .set_index("SMALL_AREA")
        .loc[:, [f"{name}_gwh_per_year", f"{name}_tj_per_km2_year"]]
    )


# %%
industrial = valuation_office_private.query("Industrial == 1")
industrial_demand = calculate_hdd_per_year(
    industrial, "industrial", dublin_small_area_boundaries_2011
)

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
public_sector = valuation_office_private.query(
    "Benchmark in @benchmarks | `Property Use` in @uses"
)
public_sector_demand = calculate_hdd_per_year(
    public_sector, "public_sector", dublin_small_area_boundaries_2011
)

# %%
non_industrial = valuation_office_private.query(
    "Use not in @industrial.Use" "& Use not in @public_sector.Use"
)
non_industrial_demand = calculate_hdd_per_year(
    non_industrial, "non_industrial", dublin_small_area_boundaries_2011
)

# %%
residential = pd.read_csv(data_dir / "dublin_indiv_hh.csv", low_memory=False)
residential_demand = calculate_hdd_per_year(
    residential, "residential", dublin_small_area_boundaries_2011
)

# %%
categories = [
    "residential",
    "non_industrial",
    "industrial",
    "public_sector",
]
small_area_heat_demands = (
    pd.concat(
        [
            residential_demand,
            non_industrial_demand,
            industrial_demand,
            public_sector_demand,
        ],
        axis="columns",
    )
    .fillna(0)
    .assign(
        total_gwh_per_year=lambda gdf: gdf[
            [c + "_gwh_per_year" for c in categories]
        ].sum(axis="columns"),
        total_tj_per_km2_year=lambda gdf: gdf[
            [c + "_tj_per_km2_year" for c in categories]
        ].sum(axis="columns"),
    )
    .reset_index()
    .merge(dublin_small_area_boundaries_2011)
)

# %%
small_area_heat_demands.to_file(
    data_dir / "dublin_small_area_hdd.geojson", driver="GeoJSON"
)

# %%
small_area_heat_demands.to_csv(data_dir / "dublin_small_area_heat_demands.csv")

# %% [markdown]
# # Plot

# %%
pandas_bokeh.output_file(html_dir / "dublin_tj_per_km2_year.html")

# %%
small_area_hdd = small_area_heat_demands[
    [c for c in small_area_heat_demands.columns if "tj_per_km2_year" in c]
    + ["geometry"]
].assign(
    feasibility=lambda gdf: pd.cut(
        gdf["total_tj_per_km2_year"],
        bins=[-np.inf, 20, 50, 120, 300, np.inf],
        labels=[
            "Not Feasible [<20 TJ/km²year]",
            "Future Potential [20-50 TJ/km²year]",
            "Feasible with Supporting Regulation [50-120 TJ/km²year]",
            "Feasible [120-300 TJ/km²year]",
            "Very Feasible [>300 TJ/km²year]",
        ],
    ),
    category=lambda gdf: gdf["feasibility"].cat.codes,
)

# %%
hovertool_string = """
<h2>@feasibility</h2>
<table>
    <tr>
        <th colspan = "2">Heat Demand Density [TJ/km²year]</th>
    </tr>
    <tr>
        <th>Total</th>
        <td>@total_tj_per_km2_year</td>
    <tr>
    <tr>
        <th>Residential</th>
        <td>@residential_tj_per_km2_year</td>
    <tr>
    <tr>
        <th>Industrial</th>
        <td>@industrial_tj_per_km2_year</td>
    <tr>
    <tr>
        <th>Non-Industrial</th>
        <td>@non_industrial_tj_per_km2_year</td>
    <tr>
    <tr>
        <th>Public Sector</th>
        <td>@public_sector_tj_per_km2_year</td>
    <tr>
</table>
"""
small_area_hdd.plot_bokeh(
    figsize=(700, 900),
    category="category",
    colormap=["#ffffb2", "#fecc5c", "#fd8d3c", "#f03b20", "#bd0026"],
    show_colorbar=False,
    fill_alpha=0.5,
    hovertool_string=hovertool_string,
)

# %%
pandas_bokeh.output_file(html_dir / "dublin_gwh_per_year.html")

# %%
small_area_hd = small_area_heat_demands[
    [c for c in small_area_heat_demands.columns if "gwh_per_year" in c] + ["geometry"]
].assign(
    total=lambda gdf: pd.cut(
        gdf["total_gwh_per_year"],
        bins=[-np.inf, 2.5, 9, 25, 40, np.inf],
    ).cat.codes,
    residential=lambda gdf: pd.cut(
        gdf["residential_gwh_per_year"],
        bins=[-np.inf, 2.5, 9, 25, 40, np.inf],
    ).cat.codes,
    industrial=lambda gdf: pd.cut(
        gdf["industrial_gwh_per_year"],
        bins=[-np.inf, 2.5, 9, 25, 40, np.inf],
    ).cat.codes,
    non_industrial=lambda gdf: pd.cut(
        gdf["non_industrial_gwh_per_year"],
        bins=[-np.inf, 2.5, 9, 25, 40, np.inf],
    ).cat.codes,
    public_sector=lambda gdf: pd.cut(
        gdf["public_sector_gwh_per_year"],
        bins=[-np.inf, 2.5, 9, 25, 40, np.inf],
    ).cat.codes,
)

# %%
hovertool_string = """
<h3>Demand [GWh/year]</h3>
<table>
    <tr>
        <th colspan = "2">Demand [GWh/year]</th>
    </tr>
    <tr>
        <th>Total</th>
        <td>@total_gwh_per_year</td>
    <tr>
    <tr>
        <th>Residential</th>
        <td>@residential_gwh_per_year</td>
    <tr>
    <tr>
        <th>Industrial</th>
        <td>@industrial_gwh_per_year</td>
    <tr>
    <tr>
        <th>Non-Industrial</th>
        <td>@non_industrial_gwh_per_year</td>
    <tr>
    <tr>
        <th>Public Sector</th>
        <td>@public_sector_gwh_per_year</td>
    <tr>
</table>
"""
small_area_hd.plot_bokeh(
    figsize=(700, 900),
    dropdown=[
        "total",
        "residential",
        "industrial",
        "non_industrial",
        "public_sector",
    ],
    fill_alpha=0.5,
    colormap=["#ffffb2", "#fecc5c", "#fd8d3c", "#f03b20", "#bd0026"],
    show_colorbar=False,
    hovertool_string=hovertool_string,
)

# %%
