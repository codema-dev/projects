# %%
from pathlib import Path
from shutil import unpack_archive
from urllib.request import urlretrieve

import geopandas as gpd
import pandas as pd
import numpy as np

from dublin_building_stock.join import get_geometries_within

data_dir = Path("../data")
years = range(2011, 2020, 1)

# %% [markdown]
# # Get Gas Demand Averages & Total buildings by year

# %%
median_resi_annual_gas_by_postcodes_filepath = (
    data_dir / "resi_median_annual_gas_by_postcodes.geojson"
)
if not median_resi_annual_gas_by_postcodes_filepath.exists():
    urlretrieve(
        url="https://zenodo.org/record/4545792/files/resi_median_annual_gas_by_postcodes.geojson",
        filename=median_resi_annual_gas_by_postcodes_filepath,
    )

median_resi_annual_gas_by_postcodes = gpd.read_file(
    median_resi_annual_gas_by_postcodes_filepath,
    driver="GeoJSON",
)
mean_resi_annual_gas_dublin = (
    median_resi_annual_gas_by_postcodes[map(str, years)].astype(np.int32).mean()
)
mean_resi_annual_gas_dublin_2019 = mean_resi_annual_gas_dublin.loc["2019"]

# %% [markdown]
# # Get 2016 Dublin Small Areas Boundaries

# %%
dublin_boundary_filepath = data_dir / "dublin_boundary.geojson"
if not dublin_boundary_filepath.exists():
    urlretrieve(
        url="https://zenodo.org/record/4577018/files/dublin_boundary.geojson",
        filename=str(dublin_boundary_filepath),
    )

dublin_boundary = gpd.read_file(dublin_boundary_filepath, driver="GeoJSON")

small_area_boundaries_filepath = (
    data_dir
    / "Small_Areas_Ungeneralised_-_OSi_National_Statistical_Boundaries_-_2015-shp"
)
if not small_area_boundaries_filepath.exists():
    urlretrieve(
        url="https://opendata.arcgis.com/datasets/c85e610da1464178a2cd84a88020c8e2_3.zip",
        filename=str(small_area_boundaries_filepath.with_suffix(".zip")),
    )
    unpack_archive(
        small_area_boundaries_filepath.with_suffix(".zip"),
        small_area_boundaries_filepath,
    )

small_area_boundaries = gpd.read_file(small_area_boundaries_filepath)[
    ["SMALL_AREA", "EDNAME", "geometry"]
].to_crs(epsg=2157)
dublin_small_area_boundaries = get_geometries_within(
    small_area_boundaries,
    dublin_boundary,
)

# %% [markdown]
# # Get Small Area 2016 Buildings total

# %%
small_areas_glossary_filepath = data_dir / "SAPS_2016_Glossary.xlsx"
if not small_areas_glossary_filepath.exists():
    urlretrieve(
        url="https://www.cso.ie/en/media/csoie/census/census2016/census2016boundaryfiles/SAPS_2016_Glossary.xlsx",
        filename=str(small_areas_glossary_filepath),
    )

small_area_glossary = (
    pd.read_excel(small_areas_glossary_filepath, engine="openpyxl")[
        ["Column Names", "Description of Field"]
    ]
    .set_index("Column Names")
    .to_dict()
)

small_areas_data_filepath = data_dir / "SAPS2016_SA2017.csv"
if not small_areas_data_filepath.exists():
    urlretrieve(
        url="https://www.cso.ie/en/media/csoie/census/census2016/census2016boundaryfiles/SAPS2016_SA2017.csv",
        filename=str(small_areas_data_filepath),
    )

small_area_2016_total_hh = (
    pd.read_csv(small_areas_data_filepath)
    .assign(SMALL_AREA=lambda df: df["GEOGID"].str[7:])[["T6_1_TH", "SMALL_AREA"]]
    .rename(columns={"T6_1_TH": "total_hh"})
)
dublin_small_area_2016_total_hh = small_area_2016_total_hh.merge(
    dublin_small_area_boundaries[["SMALL_AREA"]]
)

# %% [markdown]
# # Apply gas average to buildings to get heat demand density

# %%
small_area_resi_heat_demand_densities = (
    dublin_small_area_boundaries.merge(dublin_small_area_2016_total_hh)
    .assign(
        area_km2=lambda gdf: gdf.eval("geometry.area * 10**-6"),
        resi_heat_demand_tj_per_year=lambda gdf: gdf.eval(
            "total_hh * @mean_resi_annual_gas_dublin_2019 * 3.5 * 10**6 * 10**-12"
        ),
        resi_heat_demand_tj_per_km2_year=lambda gdf: gdf.eval(
            "resi_heat_demand_tj_per_year * area_km2"
        ),
    )
    .loc[:, ["SMALL_AREA", "resi_heat_demand_tj_per_km2_year", "geometry"]]
)

# %% [markdown]
# # Save


# %%
small_area_resi_heat_demand_densities.to_file(
    data_dir
    / "resi_heat_demand_density_2019_via_gas_average_and_2016_census_stock.geojson",
    driver="GeoJSON",
)

# %%
small_area_resi_heat_demand_densities.to_csv(
    data_dir / "resi_heat_demand_density_2019_via_gas_average_and_2016_census_stock.csv"
)
