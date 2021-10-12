from pathlib import Path

import matplotlib as plt
import pandas as pd
import seaborn as sns
import yaml

sns.set()

# + tags=["parameters"]
upstream = [
    "download_synthetic_bers",
    "download_valuation_office_energy_estimates",
    "download_epa_industrial_site_demands",
    "download_public_sector_demands",
]
external_emissions_yml = None
product = None
# -

Path(product["overall"]).parent.mkdir(exist_ok=True)  # create processed/ directroy

residential = pd.read_parquet(upstream["download_synthetic_bers"])

commercial_and_industrial = pd.read_csv(
    upstream["download_valuation_office_energy_estimates"]
)

partial_industrial = pd.read_excel(upstream["download_epa_industrial_site_demands"])

public_sector = pd.read_csv(upstream["download_public_sector_demands"])

with open(external_emissions_yml, "r") as f:
    external_emissions = yaml.safe_load(f)

## Globals

kwh_to_twh = 1e9
mwh_to_twh = 1e6
tco2_per_kwh_to_tco2_per_twh = 1e9

# source: https://www.seai.ie/data-and-insights/seai-statistics/conversion-factors/
twh_to_tco2 = {
    "Mains Gas": 204.7e3,
    "Electricity": 295.1e3,
}
mwh_to_tco2 = {
    "Mains Gas": 204.7e-3,
    "Electricity": 295.1e-3,
}
kwh_to_tco2 = {
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

## Residential

# ASSUMPTION: pumps, fans & lighting represent 20% of electrical demand
# ... Energy in Residential Sector, SEAI 2013
electrical_demand_columns = [
    c for c in residential.columns if any(x in c for x in ["pump", "lighting"])
]
residential_electricity_emissions = (
    twh_to_tco2["Electricity"]
    * residential[electrical_demand_columns].sum().sum()
    * 5
    / kwh_to_twh
)

boiler_demand_columns = [
    c for c in residential.columns if any(x in c for x in ["sh_demand", "hw_demand"])
]
emission_factors = residential["main_sh_boiler_fuel"].map(kwh_to_tco2)

residential_fossil_fuel_emissions = (
    residential[boiler_demand_columns].sum(axis=1).multiply(emission_factors).sum()
)


## Commercial

# ASSUMPTION: Average commercial boiler efficiency is the same as average residential
# ... Building Energy Ratings dataset, SEAI 2021
# assumed_boiler_efficiency is 0.85
# ASSUMPTION: Commercial fossil fuel usage entirely consists of gas
commercial_heat_emissions = (
    twh_to_tco2["Mains Gas"]
    * commercial_and_industrial["fossil_fuel_heat_demand_mwh_per_y"].sum()
    / mwh_to_twh
)

# ASSUMPTION: Commercial fossil fuel usage entirely consists of gas
commercial_fossil_fuel_emissions = (
    twh_to_tco2["Mains Gas"]
    * commercial_and_industrial["fossil_fuel_demand_mwh_per_y"].sum()
    / mwh_to_twh
)

commercial_electricity_emissions = (
    twh_to_tco2["Electricity"]
    * commercial_and_industrial["electricity_demand_mwh_per_y"].sum()
    / mwh_to_twh
)


## Industrial

### Reported site energy usage

ignore_sites = [
    "Viridian Power / Huntstown Power Station Phase 2",
    "Huntstown Power Company",
    "ESB Poolbeg Generating Station",
    "Synergen Power Ltd",
    "Dublin Aerospace Ltd.",
]
industrial_sites = partial_industrial.query("Name != @ignore_sites")

industrial_site_ids = industrial_sites["Valuation Office ID"].tolist()

industrial_electricity_emissions_epa = (
    twh_to_tco2["Electricity"]
    * industrial_sites["Electricity Use [kWh/y]"].sum()
    / kwh_to_twh
)

fossil_fuel_use_columns = [
    c
    for c in industrial_sites.columns
    if ("Use [kWh/y]" in c) and ("Electricity" not in c)
]
industrial_fossil_fuel_epa = (
    industrial_sites[fossil_fuel_use_columns].sum().sum() / kwh_to_twh
)

industrial_fossil_fuel_emissions_epa = (
    industrial_fossil_fuel_epa * twh_to_tco2["Mains Gas"]
)

# ASSUMPTION: A high proportion of these industrial boilers will be producing steam
# which means they will have a lower efficiency than a standard hot water boiler (~75%).
# Assume 80% for to account for hot water boilers.
assumed_boiler_efficiency = 0.8
industrial_heat_emissions_epa = (
    twh_to_tco2["Mains Gas"] * industrial_fossil_fuel_epa * assumed_boiler_efficiency
)

### Remaining sites are benchmark-derived

benchmark_derived_industrial_sites = commercial_and_industrial.query(
    "PropertyNo != @industrial_site_ids"
)

# ASSUMPTION: Process energy wholly consists of high and low temperature heat
# ASSUMPTION: Industrial fossil fuel usage entirely consists of gas
industrial_low_temperature_heat_emissions = (
    twh_to_tco2["Mains Gas"]
    * benchmark_derived_industrial_sites[
        "industrial_low_temperature_heat_demand_mwh_per_y"
    ].sum()
    / mwh_to_twh
)

industrial_high_temperature_heat_emissions = (
    twh_to_tco2["Mains Gas"]
    * benchmark_derived_industrial_sites[
        "industrial_high_temperature_heat_demand_mwh_per_y"
    ].sum()
    / mwh_to_twh
)

# ASSUMPTION: Industrial building electricity usage corresponds to the national split
# ... Energy in Ireland, SEAI 2021
assumed_electricity_usage = 0.6
industrial_electricity_emissions_cibse = (
    twh_to_tco2["Electricity"]
    * benchmark_derived_industrial_sites["building_energy_mwh_per_y"]
    .multiply(assumed_electricity_usage)
    .sum()
    / mwh_to_twh
)

industrial_fossil_fuel_emissions_cibse = (
    twh_to_tco2["Mains Gas"]
    * benchmark_derived_industrial_sites["building_energy_mwh_per_y"]
    .multiply(1 - assumed_electricity_usage)
    .sum()
    / mwh_to_twh
)


## Public Sector

public_sector_gas = public_sector["gas_kwh_per_year_2018"].sum() / kwh_to_twh

public_sector_gas_emissions = public_sector_gas * twh_to_tco2["Mains Gas"]

# ASSUMPTION: Average commercial boiler efficiency is the same as average residential
# ... Building Energy Ratings dataset, SEAI 2021
assumed_boiler_efficiency = 0.85
public_sector_heat_emissions = public_sector_gas * assumed_boiler_efficiency

public_sector_electricity_emissions = (
    twh_to_tco2["Electricity"]
    * public_sector["electricity_kwh_per_year_2018"].sum()
    / kwh_to_twh
)


## Rest

data_centre_emissions = external_emissions["data_centres"]

road_transport_emissions = external_emissions["road"]

rail_transport_emissions = (
    external_emissions["rail"]["DART"]
    + external_emissions["rail"]["LUAS"]
    + external_emissions["rail"]["Commuter"]
    + external_emissions["rail"]["Intercity"]
)

## Plot

### Overall

emissions = pd.Series(
    {
        "Residential": residential_electricity_emissions
        + residential_fossil_fuel_emissions,
        "Commercial": commercial_electricity_emissions
        + commercial_fossil_fuel_emissions,
        "Industrial": industrial_electricity_emissions_cibse
        + industrial_fossil_fuel_emissions_cibse
        + industrial_electricity_emissions_epa
        + industrial_fossil_fuel_emissions_epa,
        "Public Sector": public_sector_electricity_emissions
        + public_sector_gas_emissions,
        "Data Centres": data_centre_emissions,
        "Road Transport": road_transport_emissions,
        "Rail Transport": rail_transport_emissions,
    }
)

emissions.plot.pie(figsize=(10, 10), ylabel="", autopct="%1.1f%%")

emissions.to_csv(product["overall"])

### Heat : Electricity : Transport

# Ignoring electricity used for heat!
heat_vs_electricity_vs_transport = pd.Series(
    {
        "Heat": residential_fossil_fuel_emissions
        + commercial_heat_emissions
        + industrial_heat_emissions_epa
        + industrial_low_temperature_heat_emissions
        + industrial_high_temperature_heat_emissions
        + public_sector_heat_emissions,
        "Electricity": residential_electricity_emissions
        + commercial_electricity_emissions
        + industrial_electricity_emissions_epa
        + industrial_electricity_emissions_cibse
        + public_sector_electricity_emissions,
        "Transport": road_transport_emissions + rail_transport_emissions,
    }
)

heat_vs_electricity_vs_transport.plot.pie(
    figsize=(10, 10), ylabel="", autopct="%1.1f%%"
)

heat_vs_electricity_vs_transport.to_csv(product["heat_vs_electricity_vs_transport"])
