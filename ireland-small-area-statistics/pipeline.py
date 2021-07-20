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


def main(config: ConfigParser = CONFIG, data_dir: Path = DATA_DIR):

    if not data_dir.exists():
        data_dir.mkdir(exist_ok=True)

    filepaths: Dict[str, str] = {
        "small_area_statistics": str(
            data_dir / config["filenames"]["small_area_statistics"]
        ),
        "small_area_boundaries": str(
            data_dir / config["filenames"]["small_area_boundaries"]
        ),
        "routing_key_boundaries": str(
            data_dir / config["filenames"]["routing_key_boundaries"]
        ),
        "routing_key_descriptors_to_postcodes": str(
            data_dir / "routing_key_descriptors_to_postcodes.json"
        ),
        "building_ages": str(data_dir / "building_ages_2016.parquet"),
    }

    ## Transform functions into prefect tasks
    download = prefect.task(tasks.download)
    read_csv = prefect.task(pd.read_csv)
    read_shp = prefect.task(gpd.read_file)
    read_json = prefect.task(tasks.read_json)
    extract_period_built_statistics = prefect.task(
        tasks.extract_period_built_statistics,
    )
    melt_small_area_statistics_to_individual_buildings = prefect.task(
        tasks.melt_small_area_statistics_to_individual_buildings,
    )
    map_routing_keys_to_countyname = prefect.task(tasks.map_routing_keys_to_countyname)
    link_small_areas_to_routing_keys = prefect.task(
        tasks.link_small_areas_to_routing_keys
    )
    to_parquet = prefect.task(tasks.to_parquet)
    merge = prefect.task(pd.merge)

    ## Generate prefect flow
    with prefect.Flow("Ireland Small Area Statistics") as flow:

        download_small_areas_statistics = download(
            url=config["urls"]["small_area_statistics"],
            filename=filepaths["small_area_statistics"],
        )
        download_small_area_boundaries = download(
            url=config["urls"]["small_area_boundaries"],
            filename=filepaths["small_area_boundaries"],
        )
        download_routing_key_boundaries = download(
            url=config["urls"]["routing_key_boundaries"],
            filename=filepaths["routing_key_boundaries"],
        )
        download_routing_key_descriptors_to_postcodes = download(
            url=config["urls"]["routing_key_descriptors_to_postcodes"],
            filename=filepaths["routing_key_descriptors_to_postcodes"],
        )

        small_areas_statistics = read_csv(filepaths["small_area_statistics"])
        small_areas_building_ages = extract_period_built_statistics(
            small_areas_statistics
        )
        buildings_ages = melt_small_area_statistics_to_individual_buildings(
            small_areas_building_ages
        )

        routing_key_descriptors_to_postcodes = read_json(
            filepaths["routing_key_descriptors_to_postcodes"]
        )
        routing_key_boundaries = map_routing_keys_to_countyname(
            read_shp(filepaths["routing_key_boundaries"]),
            routing_key_descriptors_to_postcodes,
        )
        small_area_boundaries = read_shp(filepaths["small_area_boundaries"])
        small_areas_in_routing_keys = link_small_areas_to_routing_keys(
            small_area_boundaries, routing_key_boundaries
        )

        building_ages_in_countyname = merge(
            left=buildings_ages, right=small_areas_in_routing_keys
        )

        to_parquet(
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


if __name__ == "__main__":
    main()
