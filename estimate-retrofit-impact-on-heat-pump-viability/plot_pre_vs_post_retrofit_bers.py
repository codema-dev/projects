import geopandas as gpd
import pandas as pd

import seaborn as sns

sns.set()

# + tags=["parameters"]
upstream = [
    "download_buildings",
    "estimate_retrofit_ber_rating_improvement",
]
product = None
# -

pre_retrofit = pd.read_parquet(upstream["download_buildings"])

ber_improvement = pd.read_csv(upstream["estimate_retrofit_ber_rating_improvement"])

pre_retrofit["energy_rating"].value_counts().sort_index().plot.bar()

ber_improvement["energy_rating"].value_counts().sort_index().plot.bar()
