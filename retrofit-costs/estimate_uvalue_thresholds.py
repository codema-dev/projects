import pandas as pd

import seaborn as sns

sns.set()

# + tags=["parameters"]
upstream = ["download_buildings"]
product = None
# -

buildings = pd.read_parquet(upstream["download_buildings"])

buildings["wall_uvalue"].plot.hist(bins=30)

buildings["roof_uvalue"].plot.hist(bins=30)

buildings["window_uvalue"].plot.hist(bins=30)

buildings["wall_uvalue"].to_csv(product["wall"])

buildings["roof_uvalue"].to_csv(product["roof"])

buildings["window_uvalue"].to_csv(product["window"])
