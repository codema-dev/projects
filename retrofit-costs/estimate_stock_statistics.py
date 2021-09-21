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

## Estimate Energy & Emission Savings

# seai, 2020
emission_factors = energy_saving["main_sh_boiler_fuel"].map(
    {
        "Mains Gas": 204.7e-6,
        "Heating Oil": 263e-6,
        "Electricity": 295.1e-6,
        "Bulk LPG": 229e-6,
        "Wood Pellets (bags)": 390e-6,
        "Wood Pellets (bulk)": 160e-6,
        "Solid Multi-Fuel": 390e-6,
        "Manuf.Smokeless Fuel": 390e-6,
        "Bottled LPG": 229e-6,
        "House Coal": 340e-6,
        "Wood Logs": 390e-6,
        "Peat Briquettes": 355e-6,
        "Anthracite": 340e-6,
    }
)

energy_saving["energy_saving_kwh_per_y"].sum() / 1e9

energy_saving["energy_saving_kwh_per_y"].multiply(emission_factors).sum()

energy_saving_with_rebound["energy_saving_kwh_per_y"].sum() / 1e9

energy_saving_with_rebound["energy_saving_kwh_per_y"].multiply(emission_factors).sum()

## Save Data

small_area_total_buildings.to_csv(product["total_buildings"])

percentage_viable_for_heat_pumps.to_csv(product["heat_pump_viability"])
