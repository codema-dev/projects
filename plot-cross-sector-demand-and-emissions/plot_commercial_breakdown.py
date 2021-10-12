import pandas as pd
import seaborn as sns

sns.set()

# + tags=["parameters"]
upstream = [
    "download_valuation_office_energy_estimates",
    "download_epa_industrial_site_demands",
]
product = None
# -

## Globals

kwh_to_twh = 1e-9

mwh_to_twh = 1e-6

mwh_to_tco2 = {
    "Mains Gas": 204.7e-3,
    "Electricity": 295.1e-3,
}


## Load

epa_industrial_sites = pd.read_excel(upstream["download_epa_industrial_site_demands"])

industrial_site_ids = epa_industrial_sites["Valuation Office ID"].tolist()

# skip as have either reported data or better estimates (data centres)
skip_benchmarks = [
    "Data Centre",
    "Schools and seasonal public buildings",
    "Hospital (clinical and research)",
    "Cultural activities",
    "University campus",
    "Emergency services",
]

# skip benchmarks & buildings accounted for by EPA industrial sites
commercial_and_industrial = pd.read_csv(
    upstream["download_valuation_office_energy_estimates"]
).query("Benchmark not in @skip_benchmarks and PropertyNo not in @industrial_site_ids")


## Group By Benchmark

### Commercial

commercial_fossil_fuel = commercial_and_industrial.groupby("Benchmark")[
    "fossil_fuel_demand_mwh_per_y"
].sum()

# ASSUMPTION: Commercial fossil fuel usage entirely consists of gas
commercial_fossil_fuel_emissions = commercial_fossil_fuel * mwh_to_tco2["Mains Gas"]

commercial_electricity = commercial_and_industrial.groupby("Benchmark")[
    "electricity_demand_mwh_per_y"
].sum()

commercial_electricity_emissions = commercial_electricity * mwh_to_tco2["Electricity"]

commercial_emissions = (
    pd.concat(
        [commercial_fossil_fuel_emissions, commercial_electricity_emissions],
        keys=["Fossil Fuel", "Electricity"],
    )
    .swaplevel(0, 1)
    .sort_index()
)

### Industrial

# ASSUMPTION: Industrial building electricity usage corresponds to the national split
# ... Energy in Ireland, SEAI 2021
assumed_electricity_usage = 0.6
industrial_electricity = (
    commercial_and_industrial.groupby("Benchmark")["building_energy_mwh_per_y"].sum()
    * assumed_electricity_usage
)

industrial_electricity_emissions = industrial_electricity * mwh_to_tco2["Electricity"]

# ASSUMPTION: Process energy wholly produced using gas
industrial_fossil_fuel = (
    commercial_and_industrial.groupby("Benchmark")["building_energy_mwh_per_y"].sum()
    * (1 - assumed_electricity_usage)
    + commercial_and_industrial.groupby("Benchmark")["process_energy_mwh_per_y"].sum()
)

industrial_fossil_fuel_emissions = industrial_fossil_fuel * mwh_to_tco2["Mains Gas"]

industrial_emissions = (
    pd.concat(
        [
            industrial_electricity_emissions,
            industrial_fossil_fuel_emissions,
        ],
        keys=[
            "Fossil Fuel",
            "Electricity",
        ],
    )
    .swaplevel(0, 1)
    .sort_index()
)

## Overview

commercial_electricity.sum() * mwh_to_twh

commercial_fossil_fuel.sum() * mwh_to_twh

(commercial_electricity + commercial_fossil_fuel).sum() * mwh_to_twh

industrial_electricity.sum() * mwh_to_twh

industrial_fossil_fuel.sum() * mwh_to_twh

(industrial_electricity + industrial_fossil_fuel).sum() * mwh_to_twh

## Plot

ax = (
    commercial_emissions.sort_values(ascending=False)
    .head(24)
    .unstack()
    .plot.bar(figsize=(10, 10), ylabel="tCO2")
)
commercial_emissions.to_excel(product["commercial"])

commercial_emissions.sum()

ax = (
    industrial_emissions.sort_values(ascending=False)
    .head(11)
    .unstack()
    .plot.bar(figsize=(10, 10), ylabel="tCO2")
)
industrial_emissions.to_excel(product["industrial"])

industrial_emissions.sum()
