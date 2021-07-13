# %%
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

import pandas_bokeh

from dublin_building_stock.spatial_operations import get_geometries_within

data_dir = Path("../data")
html_dir = Path("../html")
pandas_bokeh.output_notebook()

# %%
dublin_small_area_boundaries_2011 = gpd.read_file(
    data_dir / "dublin_small_area_boundaries_2011.geojson", driver="GeoJSON"
)

# %%
dublin_municipality_boundaries = gpd.read_file(
    data_dir / "Dublin_Census2011_Admin_Counties_generalised20m"
)

# %%
dublin_indiv_hh = pd.read_csv(
    data_dir / "dublin_indiv_hh.csv", low_memory=False
).assign(
    inferred_heat_pump_readiness=lambda df: df["inferred_heat_pump_readiness"].astype(
        "float16"
    )
)

# %%
small_area_total = (
    dublin_indiv_hh.groupby("SMALL_AREA", as_index=False)
    .size()
    .rename(columns={"size": "small_area_total"})
)


# %%
heat_pump_ready_small_area_count = (
    dublin_indiv_hh.replace({False: np.nan})
    .groupby("SMALL_AREA", as_index=False)[
        ["heat_pump_ready", "inferred_heat_pump_readiness"]
    ]
    .count()
    .rename(
        columns={
            "heat_pump_ready": "number_of_heat_pump_ready_hh_sample",
            "inferred_heat_pump_readiness": "number_of_heat_pump_ready_hh_inferred",
        }
    )
)

# %%
heat_pump_ready_small_area_percent = (
    dublin_indiv_hh.groupby("SMALL_AREA", as_index=False)[
        ["inferred_heat_pump_readiness"]
    ]
    .apply(lambda x: 100 * x.sum() / len(x))
    .round()
    .rename(
        columns={
            "inferred_heat_pump_readiness": "percentage_of_heat_pump_ready_hh_inferred",
        }
    )
)


# %%
heat_pump_ready_small_areas = (
    small_area_total.merge(heat_pump_ready_small_area_count)
    .merge(heat_pump_ready_small_area_percent)
    .merge(dublin_small_area_boundaries_2011)
    .pipe(gpd.GeoDataFrame)
    .pipe(get_geometries_within, dublin_municipality_boundaries.to_crs(epsg=2157))
    .assign(
        latitude=lambda gdf: gdf.geometry.centroid.to_crs(epsg=4326).y,
        longitude=lambda gdf: gdf.geometry.centroid.to_crs(epsg=4326).x,
        percentage_of_heat_pump_ready_hh_inferred_binned=lambda gdf: pd.cut(
            gdf["percentage_of_heat_pump_ready_hh_inferred"],
            bins=[-np.inf, 25, 50, 75, np.inf],
        ).cat.codes,
    )
    .rename(
        columns={
            "COUNTYNAME": "municipality",
            "SMALL_AREA": "small_area",
            "EDNAME": "electoral_district",
        }
    )
)

# %%
heat_pump_ready_small_areas.drop(columns="geometry").to_csv(
    data_dir / "heat_pump_ready_small_areas.csv", index=False
)

# %%
pandas_bokeh.output_file(html_dir / "heat_pump_ready_hhs.html")

# %%
hovertool_string = """
    <h2>@electoral_district</h2>
    <h3>@small_area</h3>
    <table border="1">
        <tr>
            <th colspan=2>Heat Pump Ready Households</th>
        </tr>
        <tr>
            <th>Estimated<sup>1</sup></th>
            <td>@number_of_heat_pump_ready_hh_inferred</td>
        </tr>
        <tr>
            <th>Small Area Total</th>
            <td>@small_area_total</td>
        </tr>
        <tr>
            <th>Percentage</th>
            <td>@percentage_of_heat_pump_ready_hh_inferred %</td>
        </tr>
        <tr>
            <th>Sample<sup>2</sup></th>
            <td>@number_of_heat_pump_ready_hh_sample</td>
        </tr>
    </table>
    <hr>
    <small>
        <sup>1</sup> Combination of calculated & inferred values from building age
        <br>
        <sup>2</sup> Calculated using building fabric data
    </small>
"""
heat_pump_ready_small_areas.plot_bokeh(
    figsize=(700, 900),
    category="percentage_of_heat_pump_ready_hh_inferred_binned",
    hovertool_string=hovertool_string,
    colormap=["#edf8e9", "#bae4b3", "#74c476", "#238b45"],
    fill_alpha=0.5,
    show_colorbar=False,
)

# %%
