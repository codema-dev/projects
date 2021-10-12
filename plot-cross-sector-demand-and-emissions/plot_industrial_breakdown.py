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
    "Gasoil / Diesel": 264e-6,
    "Residual Oil / Fuel Oil": 273.6e-6,
}

kwh_to_twh = 1e-9

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

industrial_electricity_emissions = (
    industrial_electricity_epa * kwh_to_tco2["Electricity"]
)

industrial_fossil_fuel_emissions = industrial_fossil_fuel_epa * kwh_to_tco2["Mains Gas"]

industrial_emissions = (
    pd.concat(
        [industrial_fossil_fuel_emissions, industrial_electricity_emissions],
        keys=["Fossil Fuel", "Electricity"],
    )
    .swaplevel(0, 1)
    .sort_index()
)


## Plot

industrial_electricity_epa.plot.pie(figsize=(10, 10), ylabel="", autopct="%1.1f%%")
industrial_electricity_epa.to_csv(product["electricity"])

industrial_electricity_epa.sum() * kwh_to_twh

industrial_fossil_fuel_epa.plot.pie(figsize=(10, 10), ylabel="", autopct="%1.1f%%")
industrial_fossil_fuel_epa.to_csv(product["fossil_fuel"])

industrial_fossil_fuel_epa.sum() * kwh_to_twh

ax = industrial_emissions.unstack().plot.bar(figsize=(10, 10), ylabel="tCO2")
ax.tick_params(axis="x", rotation=45)
industrial_emissions.to_excel(product["emissions"])

industrial_emissions.sum()
