# %%
from pathlib import Path

from dublin_building_stock.boundaries import create_dublin_small_area_boundaries

data_dir = Path("../data")

# %%
dublin_boundary = gpd.read_file("dublin_boundary.geojson", driver="GeoJSON")
create_dublin_small_area_boundaries(data_dir, dublin_boundary)

# %%
