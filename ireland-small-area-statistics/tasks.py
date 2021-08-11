import geopandas as gpd
import pandas as pd
import prefect

import functions


download = prefect.task(functions.download)
read_csv = prefect.task(pd.read_csv)
read_shp = prefect.task(gpd.read_file)
read_json = prefect.task(functions.read_json)
extract_period_built_statistics = prefect.task(
    functions.extract_period_built_statistics,
)
melt_small_area_statistics_to_individual_buildings = prefect.task(
    functions.melt_small_area_statistics_to_individual_buildings,
)
map_routing_keys_to_countyname = prefect.task(functions.map_routing_keys_to_countyname)
link_small_areas_to_routing_keys = prefect.task(
    functions.link_small_areas_to_routing_keys
)
to_parquet = prefect.task(functions.to_parquet)
link_building_ages_to_countyname = prefect.task(
    functions.link_building_ages_to_countyname
)
extract_dublin = prefect.task(functions.extract_dublin)
to_file = prefect.task(functions.to_file)
