import geopandas as gpd
import pandas as pd

import seaborn as sns

sns.set()

# + tags=["parameters"]
upstream = [
    "download_buildings",
    "estimate_retrofit_costs",
]
product = None
# -

## Load

pre_retrofit = pd.read_parquet(upstream["download_buildings"])

retrofit_costs = pd.read_csv(upstream["estimate_retrofit_costs"])

## Estimate Total Retrofits by Measure

is_retrofitted_columns = [c for c in retrofit_costs.columns if "_is_retrofitted" in c]

retrofit_costs[is_retrofitted_columns].sum()

retrofit_costs[is_retrofitted_columns].any(axis=1).sum()

## Estimate All of Dublin Costs

cost_columns = [c for c in retrofit_costs.columns if "cost" in c]
retrofit_costs[cost_columns].sum().divide(1e6)

lower_cost_columns = [c for c in retrofit_costs.columns if "cost_lower" in c]
upper_cost_columns = [c for c in retrofit_costs.columns if "cost_upper" in c]

retrofit_costs[lower_cost_columns].sum().divide(1e6).sum()

retrofit_costs[upper_cost_columns].sum().divide(1e6).sum()
