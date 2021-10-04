from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

sns.set()

# + tags=["parameters"]
upstream = [
    "create_dublin_postcode_residential_gas_meters",
    "amalgamate_synthetic_ber_gas_meters_to_postcodes",
    "amalgamate_census_2016_gas_meters_to_postcodes",
]
product = None
# -

# manually create parent dir as ploomber isnt doing this
processed_dir = Path(product["csv"]).parent
processed_dir.mkdir(exist_ok=True)

cso_gas = pd.read_csv(
    upstream["create_dublin_postcode_residential_gas_meters"], index_col=0
)

ber_gas = pd.read_csv(
    upstream["amalgamate_synthetic_ber_gas_meters_to_postcodes"], index_col=0
)

census_gas = pd.read_csv(
    upstream["amalgamate_census_2016_gas_meters_to_postcodes"], index_col=0
)

ber_gas_vs_cso_gas_vs_census = pd.concat([cso_gas, ber_gas, census_gas], axis=1).dropna(
    how="any"
)

melted_ber_gas_vs_cso_gas_vs_census = (
    ber_gas_vs_cso_gas_vs_census.loc[:, ["2020", "ber_gas_meters", "census_gas_meters"]]
    .reset_index()
    .rename(
        columns={
            "index": "Postcodes",
            "2020": "Gas Networks Ireland 2020",
            "ber_gas_meters": "Codema 2021",
            "census_gas_meters": "Census 2016",
        }
    )
    .melt(id_vars="Postcodes", var_name="Source", value_name="Number of Gas Meters")
)

sns.catplot(
    data=melted_ber_gas_vs_cso_gas_vs_census,
    x="Postcodes",
    y="Number of Gas Meters",
    hue="Source",
    kind="bar",
    height=10,
)
plt.xticks(rotation=45)

melted_ber_gas_vs_cso_gas_vs_census.to_csv(product["csv"])
