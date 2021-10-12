from pathlib import Path

import matplotlib as plt
import pandas as pd
import seaborn as sns
import yaml

sns.set()

# + tags=["parameters"]
upstream = [
    "download_valuation_office_energy_estimates",
    "download_epa_industrial_site_demands",
]
external_energy_yml = None
external_emissions_yml = None
product = None
# -

partial_industrial = pd.read_excel(upstream["download_epa_industrial_site_demands"])

## Globals

kwh_to_twh = 1e9
mwh_to_twh = 1e6

## Sum Demands

ignore_sites = [
    "Viridian Power / Huntstown Power Station Phase 2",
    "Huntstown Power Company",
    "ESB Poolbeg Generating Station",
    "Synergen Power Ltd",
    "Dublin Aerospace Ltd.",
]

industrial_sites = partial_industrial.query("Name != @ignore_sites")

industrial_electricity_epa = industrial_sites.groupby("Category")[
    "Electricity Use [kWh/y]"
].sum()

industrial_fossil_fuel_epa = industrial_sites.groupby("Category")[
    "Total Fossil Fuel Use [kWh/y]"
].sum()


## Plot

industrial_electricity_epa.plot.pie(figsize=(10, 10), ylabel="", autopct="%1.1f%%")
industrial_electricity_epa.to_csv(product["electricity"])

industrial_fossil_fuel_epa.plot.pie(figsize=(10, 10), ylabel="", autopct="%1.1f%%")
industrial_fossil_fuel_epa.to_csv(product["fossil_fuel"])
