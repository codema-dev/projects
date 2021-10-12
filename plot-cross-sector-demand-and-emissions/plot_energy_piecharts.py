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
external_energy_yml = None
product = None
# -

Path(product["overall"]).parent.mkdir(exist_ok=True)  # create processed/ directroy

residential = pd.read_parquet(upstream["download_synthetic_bers"])

commercial_and_industrial = pd.read_csv(
    upstream["download_valuation_office_energy_estimates"]
)

partial_industrial = pd.read_excel(upstream["download_epa_industrial_site_demands"])

public_sector = pd.read_csv(upstream["download_public_sector_demands"])

with open(external_energy_yml, "r") as f:
    external_demand = yaml.safe_load(f)

## Globals

kwh_to_twh = 1e9
mwh_to_twh = 1e6

## Residential

boiler_demand_columns = [
    c for c in residential.columns if any(x in c for x in ["sh_demand", "hw_demand"])
]
residential_heat = residential[boiler_demand_columns].sum().sum() / kwh_to_twh

# ASSUMPTION: pumps, fans & lighting represent 20% of electrical demand
# ... Energy in Residential Sector, SEAI 2013
electrical_demand_columns = [
    c for c in residential.columns if any(x in c for x in ["pump", "lighting"])
]
residential_electricity = (
    5 * residential[electrical_demand_columns].sum().sum() / kwh_to_twh
)

## Commercial

# ASSUMPTION: Average commercial boiler efficiency is the same as average residential
# ... Building Energy Ratings dataset, SEAI 2021
# assumed_boiler_efficiency is 0.85
commercial_heat = (
    commercial_and_industrial["fossil_fuel_heat_demand_mwh_per_y"].sum() / mwh_to_twh
)

commercial_fossil_fuel = (
    commercial_and_industrial["fossil_fuel_demand_mwh_per_y"].sum() / mwh_to_twh
)

commercial_electricity = (
    commercial_and_industrial["electricity_demand_mwh_per_y"].sum() / mwh_to_twh
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

industrial_electricity_epa = (
    industrial_sites["Electricity Use [kWh/y]"].sum() / kwh_to_twh
)

fossil_fuel_use_columns = [
    c
    for c in industrial_sites.columns
    if ("Use [kWh/y]" in c) and ("Electricity" not in c)
]
industrial_fossil_fuel_epa = (
    industrial_sites[fossil_fuel_use_columns].sum().sum() / kwh_to_twh
)

# ASSUMPTION: A high proportion of these industrial boilers will be producing steam
# which means they will have a lower efficiency than a standard hot water boiler (~75%).
# Assume 80% for to account for hot water boilers.
assumed_boiler_efficiency = 0.8
industrial_heat_epa = industrial_fossil_fuel_epa * assumed_boiler_efficiency

industrial_energy_epa = industrial_electricity_epa + industrial_fossil_fuel_epa

### Remaining sites are benchmark-derived

benchmark_derived_industrial_sites = commercial_and_industrial.query(
    "PropertyNo != @industrial_site_ids"
)

# ASSUMPTION: Process energy wholly consists of high and low temperature heat
industrial_low_temperature_heat = (
    benchmark_derived_industrial_sites[
        "industrial_low_temperature_heat_demand_mwh_per_y"
    ].sum()
    / mwh_to_twh
)

industrial_high_temperature_heat = (
    benchmark_derived_industrial_sites[
        "industrial_high_temperature_heat_demand_mwh_per_y"
    ].sum()
    / mwh_to_twh
)

# ASSUMPTION: Industrial building electricity usage corresponds to the national split
# ... Energy in Ireland, SEAI 2021
assumed_electricity_usage = 0.6
industrial_electricity_cibse = (
    benchmark_derived_industrial_sites["building_energy_mwh_per_y"]
    .multiply(assumed_electricity_usage)
    .sum()
    / mwh_to_twh
)

industrial_fossil_fuel_cibse = (
    benchmark_derived_industrial_sites["building_energy_mwh_per_y"]
    .multiply(1 - assumed_electricity_usage)
    .sum()
    / mwh_to_twh
)

industrial_energy_cibse = (
    industrial_electricity_cibse
    + industrial_fossil_fuel_cibse
    + industrial_low_temperature_heat
    + industrial_high_temperature_heat
)

## Public Sector

public_sector_gas = public_sector["gas_kwh_per_year_2018"].sum() / kwh_to_twh

# ASSUMPTION: Average commercial boiler efficiency is the same as average residential
# ... Building Energy Ratings dataset, SEAI 2021
assumed_boiler_efficiency = 0.85
public_sector_heat = public_sector_gas * assumed_boiler_efficiency


public_sector_electricity = (
    public_sector["electricity_kwh_per_year_2018"].sum() / kwh_to_twh
)

## Rest

# Ireland’s Data Hosting Industry Biannual Report, Host In Ireland, May 2021
data_centre_electricity = external_demand["data_centres"]

road_transport_energy = external_demand["road"]

rail_transport_energy = (
    external_demand["rail"]["DART"]
    + external_demand["rail"]["LUAS"]
    + external_demand["rail"]["Commuter"]
    + external_demand["rail"]["Intercity"]
)

rail_transport_electricity = (
    external_demand["rail"]["DART"] + external_demand["rail"]["LUAS"]
)


## Plot

### Overall

energy = pd.Series(
    {
        "Residential": residential_electricity + residential_heat,
        "Commercial": commercial_electricity + commercial_fossil_fuel,
        "Industrial": industrial_energy_epa + industrial_energy_cibse,
        "Public Sector": public_sector_electricity + public_sector_gas,
        "Data Centres": data_centre_electricity,
        "Road Transport": road_transport_energy,
        "Rail Transport": rail_transport_energy,
    }
)

energy.plot.pie(figsize=(10, 10), ylabel="", autopct="%1.1f%%")

energy.to_csv(product["overall"])

### Heat : Electricity : Transport

# Ignoring electricity used for heat!
heat_vs_electricity_vs_transport = pd.Series(
    {
        "Heat": residential_heat
        + commercial_heat
        + industrial_low_temperature_heat
        + industrial_high_temperature_heat
        + industrial_heat_epa
        + public_sector_heat,
        "Electricity": residential_electricity
        + commercial_electricity
        + industrial_electricity_epa
        + industrial_electricity_cibse
        + public_sector_electricity,
        "Road Transport": road_transport_energy + rail_transport_energy,
    }
)

heat_vs_electricity_vs_transport.plot.pie(
    figsize=(10, 10), ylabel="", autopct="%1.1f%%"
)

heat_vs_electricity_vs_transport.to_csv(product["heat_vs_electricity_vs_transport"])


## Electricity

# Ignoring road transport as don't have EV vs Fossil Fuel Breakdown
electricity = pd.Series(
    {
        "Residential": residential_electricity,
        "Commercial": commercial_electricity,
        "Industrial": industrial_electricity_epa + industrial_electricity_cibse,
        "Public Sector": public_sector_electricity,
        "Rail Transport": rail_transport_electricity,
        "Data Centres": data_centre_electricity,
    }
)

electricity.plot.pie(figsize=(10, 10), ylabel="", autopct="%1.1f%%")

electricity.to_csv(product["electricity"])

## Heat

# Ignoring road transport as don't have EV vs Fossil Fuel Breakdown
heat = pd.Series(
    {
        "Residential": residential_heat,
        "Commercial": commercial_heat,
        "Industrial": industrial_heat_epa
        + industrial_low_temperature_heat
        + industrial_high_temperature_heat,
        "Public Sector": public_sector_heat,
    }
)

heat.plot.pie(figsize=(10, 10), ylabel="", autopct="%1.1f%%")

heat.to_csv(product["heat"])
