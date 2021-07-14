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


CONFIG = ConfigParser()
CONFIG.read("config.ini")
DATA_DIR = Path(__name__).parent / "data"


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
        "counties": str(data_dir.parent / "counties.json"),
        "inferred_building_ages": str(
            data_dir / config["filenames"]["inferred_building_ages_2016"]
        ),
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
    replace_not_stated_period_built_with_mode = prefect.task(
        tasks.replace_not_stated_period_built_with_mode,
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

        small_areas_statistics = read_csv(filepaths["small_area_statistics"])
        small_areas_building_ages = extract_period_built_statistics(
            small_areas_statistics
        )
        buildings_ages = melt_small_area_statistics_to_individual_buildings(
            small_areas_building_ages
        )
        inferred_building_ages = replace_not_stated_period_built_with_mode(
            buildings_ages
        )

        counties = read_json(filepaths["counties"])
        routing_key_boundaries = map_routing_keys_to_countyname(
            read_shp(filepaths["routing_key_boundaries"]), counties
        )
        small_area_boundaries = read_shp(filepaths["small_area_boundaries"])
        small_areas_in_routing_keys = link_small_areas_to_routing_keys(
            small_area_boundaries, routing_key_boundaries
        )

        inferred_building_ages_in_countyname = merge(
            left=inferred_building_ages, right=small_areas_in_routing_keys
        )

        to_parquet(
            inferred_building_ages_in_countyname,
            path=filepaths["inferred_building_ages"],
        )

        ## Manually set dependencies where prefect can't infer run-order
        small_areas_statistics.set_upstream(download_small_areas_statistics)
        small_area_boundaries.set_upstream(download_small_area_boundaries)
        routing_key_boundaries.set_upstream(download_routing_key_boundaries)

    ## Run flow!
    from prefect.utilities.debug import raise_on_exception

    with raise_on_exception():
        flow.run()


if __name__ == "__main__":
    main()
