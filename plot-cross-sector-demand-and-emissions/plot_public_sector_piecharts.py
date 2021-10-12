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

## Group By Use Types

public_sector_gas = public_sector.groupby("category")["gas_kwh_per_year_2018"].sum()

public_sector_electricity = public_sector.groupby("category")[
    "electricity_kwh_per_year_2018"
].sum()

## Plot

public_sector_electricity.plot.pie(figsize=(10, 10), ylabel="", autopct="%1.1f%%")
public_sector_electricity.to_csv(product["electricity"])

public_sector_gas.plot.pie(figsize=(10, 10), ylabel="", autopct="%1.1f%%")
public_sector_gas.to_csv(product["fossil_fuel"])
