import geopandas as gpd
import prefect

import functions

check_file_exists = prefect.task(functions.check_file_exists)
read_file = prefect.task(gpd.read_file)
read_network = prefect.task(functions.read_network)
