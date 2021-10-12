from pathlib import Path

import matplotlib as plt
import pandas as pd
import seaborn as sns

sns.set()

# + tags=["parameters"]
upstream = [
    "download_public_sector_demands",
]
product = None
# -

## Load

public_sector = pd.read_csv(upstream["download_public_sector_demands"])

## Globals

kwh_to_tco2 = {"Mains Gas": 204.7e-6, "Electricity": 295.1e-6}

kwh_to_twh = 1e-9

## Group By Use Types

public_sector_gas = public_sector.groupby("category")["gas_kwh_per_year_2018"].sum()

public_sector_gas_emissions = public_sector_gas * kwh_to_tco2["Mains Gas"]

public_sector_electricity = public_sector.groupby("category")[
    "electricity_kwh_per_year_2018"
].sum()

public_sector_electricity_emissions = public_sector_gas * kwh_to_tco2["Electricity"]

public_sector_emissions = (
    pd.concat(
        [public_sector_gas_emissions, public_sector_electricity_emissions],
        keys=["Gas", "Electricity"],
    )
    .swaplevel(0, 1)
    .sort_index()
)

## Plot

public_sector_electricity.plot.pie(figsize=(10, 10), ylabel="", autopct="%1.1f%%")
public_sector_electricity.to_csv(product["electricity"])

public_sector_electricity.sum() / 1e9

public_sector_gas.plot.pie(figsize=(10, 10), ylabel="", autopct="%1.1f%%")
public_sector_gas.to_csv(product["fossil_fuel"])

public_sector_gas.sum() * kwh_to_twh

ax = public_sector_emissions.unstack().plot.bar(figsize=(10, 10), ylabel="tCO2")
ax.tick_params(axis="x", rotation=45)
public_sector_emissions.to_excel(product["emissions"])

public_sector_emissions.sum()
