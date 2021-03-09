from pathlib import Path
from shutil import unpack_archive
from urllib.request import urlretrieve

import geopandas as gpd
import pandas as pd
import numpy as np
from valuation_office_ireland.download import download_valuation_office_categories

from dublin_building_stock.join import get_geometries_within


def get_dublin_boundary(data_dir):

    dublin_boundary_filepath = data_dir / "dublin_boundary.geojson"
    if not dublin_boundary_filepath.exists():
        urlretrieve(
            url="https://zenodo.org/record/4577018/files/dublin_boundary.geojson",
            filename=str(dublin_boundary_filepath),
        )

    return gpd.read_file(dublin_boundary_filepath, driver="GeoJSON")


def get_dublin_small_area_boundaries(data_dir):

    dublin_small_area_boundaries_filepath = data_dir / "dublin_small_area_boundaries"
    if dublin_small_area_boundaries_filepath.exists():
        dublin_small_area_boundaries_filepath = gpd.read_file(
            dublin_small_area_boundaries_filepath,
        )
    else:
        dublin_boundary = get_dublin_boundary(data_dir)
        ireland_small_area_boundaries = get_ireland_small_area_boundaries(data_dir)
        dublin_small_area_boundaries = get_geometries_within(
            ireland_small_area_boundaries,
            dublin_boundary,
        )
        dublin_small_area_boundaries.to_file(
            dublin_small_area_boundaries_filepath,
        )

    return dublin_small_area_boundaries


def get_ireland_small_area_boundaries(data_dir):

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

    return gpd.read_file(small_area_boundaries_filepath)[
        ["SMALL_AREA", "EDNAME", "geometry"]
    ].to_crs(epsg=2157)


def get_valuation_office_buildings(data_dir, local_authorities):
    valuation_office_filepath = data_dir / "valuation_office"
    if not valuation_office_filepath.exists():
        download_valuation_office_categories(
            savedir=data_dir,
            local_authorities=local_authorities,
        )

    return pd.concat(
        [pd.read_csv(filepath) for filepath in valuation_office_filepath.glob("*.csv")]
    ).reset_index(drop=True)
