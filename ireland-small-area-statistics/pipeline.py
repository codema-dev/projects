from configparser import ConfigParser
from os import link
from pathlib import Path
from typing import Dict
from typing import Union

from dotenv import load_dotenv

import geopandas as gpd
import pandas as pd

load_dotenv(".prefect")  # must load prefect environmental variables prior to import!
import prefect
from prefect.engine.results import LocalResult
from prefect.engine.serializers import PandasSerializer

import tasks

HERE = Path(__name__).parent
CONFIG = ConfigParser()
CONFIG.read(HERE / "config.ini")
DATA_DIR = HERE / "data"


filepaths: Dict[str, str] = {
    "small_area_statistics": str(
        DATA_DIR / "external" / CONFIG["filenames"]["small_area_statistics"]
    ),
    "small_area_boundaries": str(
        DATA_DIR / "external" / CONFIG["filenames"]["small_area_boundaries"]
    ),
    "routing_key_boundaries": str(
        DATA_DIR / "external" / CONFIG["filenames"]["routing_key_boundaries"]
    ),
    "routing_key_descriptors_to_postcodes": str(
        DATA_DIR / "external" / "routing_key_descriptors_to_postcodes.json"
    ),
    "building_ages": str(DATA_DIR / "processed" / "building_ages_2016.parquet"),
    "dublin_small_areas": str(
        DATA_DIR / "processed" / "dublin_small_area_boundaries_in_routing_keys.gpkg"
    ),
}


## Generate prefect flow
with prefect.Flow("Ireland Small Area Statistics") as flow:

    download_small_areas_statistics = tasks.download(
        url=CONFIG["urls"]["small_area_statistics"],
        filename=filepaths["small_area_statistics"],
    )
    download_small_area_boundaries = tasks.download(
        url=CONFIG["urls"]["small_area_boundaries"],
        filename=filepaths["small_area_boundaries"],
    )
    download_routing_key_boundaries = tasks.download(
        url=CONFIG["urls"]["routing_key_boundaries"],
        filename=filepaths["routing_key_boundaries"],
    )
    download_routing_key_descriptors_to_postcodes = tasks.download(
        url=CONFIG["urls"]["routing_key_descriptors_to_postcodes"],
        filename=filepaths["routing_key_descriptors_to_postcodes"],
    )

    small_areas_statistics = tasks.read_csv(filepaths["small_area_statistics"])
    small_areas_building_ages = tasks.extract_period_built_statistics(
        small_areas_statistics
    )
    buildings_ages = tasks.melt_small_area_statistics_to_individual_buildings(
        small_areas_building_ages
    )

    routing_key_descriptors_to_postcodes = tasks.read_json(
        filepaths["routing_key_descriptors_to_postcodes"]
    )
    routing_key_boundaries = tasks.map_routing_keys_to_countyname(
        tasks.read_shp(filepaths["routing_key_boundaries"]),
        routing_key_descriptors_to_postcodes,
    )
    small_area_boundaries = tasks.read_shp(filepaths["small_area_boundaries"])
    small_areas_in_routing_keys = tasks.link_small_areas_to_routing_keys(
        small_area_boundaries, routing_key_boundaries
    )
    dublin_small_areas = tasks.extract_dublin(small_areas_in_routing_keys)

    building_ages_in_countyname = tasks.link_building_ages_to_countyname(
        buildings_ages, small_areas_in_routing_keys
    )

    tasks.to_file(
        dublin_small_areas, path=filepaths["dublin_small_areas"], driver="GPKG"
    )
    tasks.to_parquet(
        building_ages_in_countyname,
        path=filepaths["building_ages"],
    )

    ## Manually set dependencies where prefect can't infer run-order
    small_areas_statistics.set_upstream(download_small_areas_statistics)
    small_area_boundaries.set_upstream(download_small_area_boundaries)
    routing_key_boundaries.set_upstream(download_routing_key_boundaries)
    routing_key_descriptors_to_postcodes.set_upstream(
        download_routing_key_descriptors_to_postcodes
    )

## Run flow!
from prefect.utilities.debug import raise_on_exception

with raise_on_exception():
    flow.run()
