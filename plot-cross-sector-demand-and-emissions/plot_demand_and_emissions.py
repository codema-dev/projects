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
external_demand_and_emissions_yml = None
product = None
# -

residential = pd.read_parquet(upstream["download_synthetic_bers"])

commercial_and_industrial = pd.read_csv(
    upstream["download_valuation_office_energy_estimates"]
)

partial_industrial = pd.read_csv(upstream["download_epa_industrial_site_demands"])

public_sector = pd.read_csv(upstream["download_public_sector_demands"])

with open(external_demand_and_emissions_yml, "r") as f:
    external_demand_and_emissions = yaml.safe_load(f)

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
assumed_boiler_efficiency = 0.85
commercial_heat = (
    commercial_and_industrial["fossil_fuel_demand_mwh_per_y"]
    .multiply(assumed_boiler_efficiency)
    .sum()
    / mwh_to_twh
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

industrial_energy_epa = industrial_electricity_epa + industrial_fossil_fuel_epa

### Remaining sites are benchmark-derived

benchmark_derived_industrial_sites = commercial_and_industrial.query(
    "PropertyNo != @industrial_site_ids"
)

industrial_low_temperature_heat = benchmark_derived_industrial_sites[
    "industrial_low_temperature_heat_demand_mwh_per_y"
].sum()

industrial_high_temperature_heat = benchmark_derived_industrial_sites[
    "industrial_high_temperature_heat_demand_mwh_per_y"
].sum()

# ASSUMPTION: Industrial building electricity usage corresponds to the national split
# ... Energy in Ireland, SEAI 2021
assumed_electricity_usage = 0.6
industrial_electricity_cibse = (
    benchmark_derived_industrial_sites["building_energy_mwh_per_y"]
    .multiply(assumed_electricity_usage)
    .sum()
    / mwh_to_twh
)

industrial_energy_cibse = (
    benchmark_derived_industrial_sites["building_energy_mwh_per_y"]
    .add(benchmark_derived_industrial_sites["process_energy_mwh_per_y"])
    .sum()
    / mwh_to_twh
)

## Public Sector

public_sector_gas = public_sector["gas_kwh_per_year_2018"].sum() / kwh_to_twh

public_sector_electricity = (
    public_sector["electricity_kwh_per_year_2018"].sum() / kwh_to_twh
)

## Rest

data_centre_electricity = external_demand_and_emissions["data_centres"]["TWh"]

road_transport_energy = external_demand_and_emissions["road"]["TWh"]

rail_transport_energy = (
    external_demand_and_emissions["rail"]["DART"]["TWh"]
    + external_demand_and_emissions["rail"]["LUAS"]["TWh"]
    + external_demand_and_emissions["rail"]["Commuter"]["TWh"]
    + external_demand_and_emissions["rail"]["Intercity"]["TWh"]
)

## Plot

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
