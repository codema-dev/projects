from pathlib import Path

from bokeh.io import export_svg
from bokeh.io import save
from bokeh.io import show
import geopandas as gpd
import numpy as np
import pandas as pd
import pandas_bokeh

from globals import DATA_DIR

## Parametrize
# overwrite parameters with arguemnts generated in prefect pipeline

# + tags=["parameters"]
SAVE_AS_HTML: bool = True
DATA_DIR: str = DATA_DIR
emission_map_filepath: str = (
    DATA_DIR / "processed" / "dublin_small_area_emissions_tco2_per_y.geojson"
)
# -

## Load

emission_map = gpd.read_file(emission_map_filepath)

## Set Globals

local_authorities = emission_map["local_authority"].unique()
