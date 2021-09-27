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

emission_factors = residential["main_sh_boiler_fuel"].map(kwh_to_tco2)
residential_emissions = (
    residential[boiler_demand_columns].sum(axis=1).multiply(emission_factors).sum()
    + residential_electricity * twh_to_tco2["Electricity"]
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

# ASSUMPTION: Commercial fossil fuel usage entirely consists of gas
commercial_emissions = (
    commercial_fossil_fuel * twh_to_tco2["Mains Gas"]
    + commercial_electricity * twh_to_tco2["Electricity"]
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

industrial_emissions_epa = (
    industrial_fossil_fuel_epa * twh_to_tco2["Mains Gas"]
    + industrial_electricity_epa * twh_to_tco2["Electricity"]
)

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

# ASSUMPTION: Industrial fossil fuel usage entirely consists of gas
industrial_emissions_cibse = (
    industrial_fossil_fuel_epa * twh_to_tco2["Mains Gas"]
    + industrial_electricity_cibse * twh_to_tco2["Electricity"]
    + industrial_low_temperature_heat * twh_to_tco2["Mains Gas"]
    + industrial_high_temperature_heat * twh_to_tco2["Mains Gas"]
)

## Public Sector

public_sector_gas = public_sector["gas_kwh_per_year_2018"].sum() / kwh_to_twh

public_sector_electricity = (
    public_sector["electricity_kwh_per_year_2018"].sum() / kwh_to_twh
)

public_sector_emissions = (
    public_sector_gas * twh_to_tco2["Mains Gas"]
    + public_sector_electricity * twh_to_tco2["Electricity"]
)

## Rest

data_centre_electricity = external_demand_and_emissions["data_centres"]["TWh"]

data_centre_emissions = external_demand_and_emissions["data_centres"]["tCO2"]

road_transport_energy = external_demand_and_emissions["road"]["TWh"]

road_transport_emissions = external_demand_and_emissions["road"]["tCO2"]

rail_transport_energy = (
    external_demand_and_emissions["rail"]["DART"]["TWh"]
    + external_demand_and_emissions["rail"]["LUAS"]["TWh"]
    + external_demand_and_emissions["rail"]["Commuter"]["TWh"]
    + external_demand_and_emissions["rail"]["Intercity"]["TWh"]
)

rail_transport_emissions = (
    external_demand_and_emissions["rail"]["DART"]["tCO2"]
    + external_demand_and_emissions["rail"]["LUAS"]["tCO2"]
    + external_demand_and_emissions["rail"]["Commuter"]["tCO2"]
    + external_demand_and_emissions["rail"]["Intercity"]["tCO2"]
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

emissions = pd.Series(
    {
        "Residential": residential_emissions,
        "Commercial": commercial_emissions,
        "Industrial": industrial_emissions_cibse + industrial_emissions_epa,
        "Public Sector": public_sector_emissions,
        "Data Centres": data_centre_emissions,
        "Road Transport": road_transport_emissions,
        "Rail Transport": rail_transport_emissions,
    }
)

emissions.plot.pie(figsize=(10, 10), ylabel="", autopct="%1.1f%%")
