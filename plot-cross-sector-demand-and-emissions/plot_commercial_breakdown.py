import pandas as pd
import seaborn as sns

sns.set()

# + tags=["parameters"]
upstream = ["download_valuation_office_energy_estimates"]
product = None
# -

## Globals

kwh_to_twh = 1e9

mwh_to_twh = 1e6

mwh_to_tco2 = {
    "Mains Gas": 204.7e-3,
    "Electricity": 295.1e-3,
}


## Load

# skip as have either reported data or better estimates (data centres)
skip_benchmarks = [
    "Data Centre",
    "Schools and seasonal public buildings",
    "Hospital (clinical and research)",
    "Cultural activities",
    "University campus",
    "Emergency services",
]
commercial_and_industrial = pd.read_csv(
    upstream["download_valuation_office_energy_estimates"]
).query("Benchmark not in @skip_benchmarks")


## Group By Benchmark

# ASSUMPTION: Commercial fossil fuel usage entirely consists of gas
commercial_fossil_fuel_emissions = (
    commercial_and_industrial.groupby("Benchmark")["fossil_fuel_demand_mwh_per_y"].sum()
    * mwh_to_tco2["Mains Gas"]
)

commercial_electricity_emissions = (
    commercial_and_industrial.groupby("Benchmark")["electricity_demand_mwh_per_y"].sum()
    * mwh_to_tco2["Electricity"]
)

commercial_emissions = (
    pd.concat(
        [commercial_fossil_fuel_emissions, commercial_electricity_emissions],
        keys=["Fossil Fuel", "Electricity"],
    )
    .swaplevel(0, 1)
    .sort_index()
)


# ASSUMPTION: Process energy wholly consists of high and low temperature heat
# ASSUMPTION: Industrial fossil fuel usage entirely consists of gas
industrial_low_temperature_heat_emissions = (
    commercial_and_industrial.groupby("Benchmark")[
        "industrial_low_temperature_heat_demand_mwh_per_y"
    ].sum()
    * mwh_to_tco2["Mains Gas"]
)

industrial_high_temperature_heat_emissions = (
    commercial_and_industrial.groupby("Benchmark")[
        "industrial_high_temperature_heat_demand_mwh_per_y"
    ].sum()
    * mwh_to_tco2["Mains Gas"]
)

# ASSUMPTION: Industrial building electricity usage corresponds to the national split
# ... Energy in Ireland, SEAI 2021
assumed_electricity_usage = 0.6
industrial_electricity_emissions = (
    commercial_and_industrial.groupby("Benchmark")["building_energy_mwh_per_y"].sum()
    * assumed_electricity_usage
    * mwh_to_tco2["Electricity"]
)

industrial_fossil_fuel_emissions = (
    commercial_and_industrial.groupby("Benchmark")["building_energy_mwh_per_y"].sum()
    * (1 - assumed_electricity_usage)
    * mwh_to_tco2["Mains Gas"]
)

industrial_emissions = (
    pd.concat(
        [
            industrial_low_temperature_heat_emissions,
            industrial_high_temperature_heat_emissions,
            industrial_electricity_emissions,
            industrial_fossil_fuel_emissions,
        ],
        keys=[
            "Low Temperature Heat",
            "High Temperature Heat",
            "Fossil Fuel",
            "Electricity",
        ],
    )
    .swaplevel(0, 1)
    .sort_index()
)

## Plot

ax = (
    commercial_emissions.sort_values(ascending=False)
    .head(24)
    .unstack()
    .plot.bar(figsize=(10, 10), ylabel="tCO2")
)
commercial_emissions.to_excel(product["commercial"])

industrial_emissions.sum()

ax = (
    industrial_emissions.sort_values(ascending=False)
    .head(20)
    .unstack()
    .plot.bar(figsize=(10, 10), ylabel="tCO2")
)
industrial_emissions.to_excel(product["industrial"])

industrial_emissions.sum()
