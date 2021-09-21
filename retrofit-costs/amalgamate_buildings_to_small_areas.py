import geopandas as gpd
import pandas as pd

import seaborn as sns

sns.set()

# + tags=["parameters"]
upstream = [
    "estimate_retrofit_costs",
    "estimate_retrofit_energy_saving",
    "estimate_retrofit_energy_saving_with_rebound",
    "estimate_retrofit_hlp_improvement",
    "estimate_retrofit_ber_rating_improvement",
    "download_small_area_boundaries",
]
product = None
# -

## Load Results

retrofit_costs = pd.read_csv(upstream["estimate_retrofit_costs"])

energy_saving = pd.read_csv(upstream["estimate_retrofit_energy_saving"])

energy_saving_with_rebound = pd.read_csv(
    upstream["estimate_retrofit_energy_saving_with_rebound"]
)

hlp_improvement = pd.read_csv(upstream["estimate_retrofit_hlp_improvement"])

ber_improvement = pd.read_csv(upstream["estimate_retrofit_ber_rating_improvement"])

small_area_boundaries = gpd.read_file(upstream["download_small_area_boundaries"])

## Plot Post-Retrofit BERs

ber_improvement["energy_rating"].value_counts().sort_index().plot.bar()


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

small_area_boundaries.merge(percentage_viable_for_heat_pumps).plot(
    column="percentage_viable_for_heat_pumps",
    figsize=(20, 20),
    legend=True,
    linewidth=0.5,
)

percentage_viable_for_heat_pumps.to_csv(product["heat_pump_viability"])
