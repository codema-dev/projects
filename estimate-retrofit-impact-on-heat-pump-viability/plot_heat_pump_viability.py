import geopandas as gpd
import pandas as pd

import seaborn as sns

sns.set()

# + tags=["parameters"]
upstream = [
    "download_small_area_boundaries",
    "estimate_retrofit_hlp_improvement",
    "estimate_retrofit_ber_rating_improvement",
]
product = None
# -

## Load

small_area_boundaries = gpd.read_file(upstream["download_small_area_boundaries"])

hlp_improvement = pd.read_csv(upstream["estimate_retrofit_hlp_improvement"])

ber_improvement = pd.read_csv(upstream["estimate_retrofit_ber_rating_improvement"])

## Map Heat Pump Viability

hlp_improvement["is_viable_for_a_heat_pump"] = (
    hlp_improvement["post_retrofit_heat_loss_parameter"] < 2
)

hlp_improvement["is_viable_for_a_heat_pump"].sum() / len(hlp_improvement)

small_areas_viable_for_heat_pumps = hlp_improvement.groupby("small_area")[
    "is_viable_for_a_heat_pump"
].sum()

small_area_total_buildings = hlp_improvement.groupby("small_area").size()

percentage_viable_for_heat_pumps = (
    small_areas_viable_for_heat_pumps.divide(small_area_total_buildings)
    .multiply(100)
    .rename("percentage_viable_for_heat_pumps")
    .reset_index()
)

heat_pump_viability_map = small_area_boundaries.merge(percentage_viable_for_heat_pumps)

heat_pump_viability_map.plot(
    column="percentage_viable_for_heat_pumps",
    figsize=(20, 20),
    legend=True,
    linewidth=0.5,
)

## Save Data

percentage_viable_for_heat_pumps.to_csv(product["csv"])
