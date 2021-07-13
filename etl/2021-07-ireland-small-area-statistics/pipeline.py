from configparser import ConfigParser
from pathlib import Path

from dotenv import load_dotenv

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

    ## Transform functions into prefect tasks
    download = prefect.task(tasks.download)
    read_csv = prefect.task(pd.read_csv)
    extract_period_built_statistics = prefect.task(
        tasks.extract_period_built_statistics,
    )
    melt_small_area_statistics_to_individual_buildings = prefect.task(
        tasks.melt_small_area_statistics_to_individual_buildings,
    )
    replace_not_stated_period_built_with_mode = prefect.task(
        tasks.replace_not_stated_period_built_with_mode,
    )
    to_parquet = prefect.task(tasks.to_parquet)

    ## Generate prefect flow
    with prefect.Flow("Ireland Small Area Statistics") as flow:

        download_small_areas_statistics = download(
            url=config["urls"]["small_area_statistics"],
            filename=str(data_dir / config["filenames"]["small_area_statistics"]),
        )
        small_areas_statistics = read_csv(
            str(data_dir / config["filenames"]["small_area_statistics"])
        )
        small_areas_statistics.set_upstream(download_small_areas_statistics)

        small_areas_building_ages = extract_period_built_statistics(
            small_areas_statistics
        )
        buildings_ages = melt_small_area_statistics_to_individual_buildings(
            small_areas_building_ages
        )
        inferred_building_ages = replace_not_stated_period_built_with_mode(
            buildings_ages
        )

        to_parquet(
            inferred_building_ages,
            path=str(data_dir / config["filenames"]["inferred_building_ages_2016"]),
        )

    ## Run flow!
    flow.run()


if __name__ == "__main__":
    main()
