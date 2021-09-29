import geopandas as gpd
import pandas as pd

import seaborn as sns

sns.set()

# + tags=["parameters"]
upstream = [
    "download_buildings",
    "estimate_retrofit_costs",
    "estimate_retrofit_energy_saving",
    "estimate_retrofit_energy_saving_with_rebound",
    "estimate_retrofit_ber_rating_improvement",
]
product = None
# -

## Load

pre_retrofit = pd.read_parquet(upstream["download_buildings"])

retrofit_costs = pd.read_csv(upstream["estimate_retrofit_costs"])

energy_saving = pd.read_csv(upstream["estimate_retrofit_energy_saving"])

energy_saving_with_rebound = pd.read_csv(
    upstream["estimate_retrofit_energy_saving_with_rebound"]
)

ber_improvement = pd.read_csv(upstream["estimate_retrofit_ber_rating_improvement"])

## Plot Post-Retrofit BERs

ber_improvement["energy_rating"].value_counts().sort_index().plot.bar()

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

energy_saving_twh = energy_saving["energy_saving_kwh_per_y"].sum() / 1e9
energy_saving_twh

energy_saving["energy_saving_kwh_per_y"].multiply(emission_factors).sum()

energy_saving_with_rebound_twh = (
    energy_saving_with_rebound["energy_saving_kwh_per_y"].sum() / 1e9
)
energy_saving_with_rebound_twh

energy_saving_with_rebound["energy_saving_kwh_per_y"].multiply(emission_factors).sum()

## Estimate Retrofitting Impact on Space Heat : Hot Water

### Pre Retrofit

pre_retrofit_sh = pre_retrofit[["main_sh_demand", "suppl_sh_demand"]].sum().sum() / 1e9

pre_retrofit_hw = pre_retrofit[["main_hw_demand", "suppl_hw_demand"]].sum().sum() / 1e9

pre_retrofit_sh_vs_hw = pd.Series(
    {"Space Heating": pre_retrofit_sh, "Hot Water": pre_retrofit_hw}
)

pre_retrofit_sh_vs_hw.plot.pie(figsize=(10, 10), ylabel="", autopct="%1.1f%%")

### Post Retrofit

pre_retrofit_sh_vs_hw = pd.Series(
    {"Space Heating": pre_retrofit_sh - energy_saving_twh, "Hot Water": pre_retrofit_hw}
)

pre_retrofit_sh_vs_hw.plot.pie(
    figsize=(10, 10), ylabel="", autopct="%1.1f%%", title="Theoretical Savings"
)

pre_retrofit_sh_vs_hw_with_rebound = pd.Series(
    {
        "Space Heating": pre_retrofit_sh - energy_saving_with_rebound_twh,
        "Hot Water": pre_retrofit_hw,
    }
)

pre_retrofit_sh_vs_hw_with_rebound.plot.pie(
    figsize=(10, 10),
    ylabel="",
    autopct="%1.1f%%",
    title="With Rebound Effect Considered",
)
