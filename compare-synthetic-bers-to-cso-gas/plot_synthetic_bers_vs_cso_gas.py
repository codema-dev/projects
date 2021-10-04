from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

sns.set()

# + tags=["parameters"]
upstream = [
    "create_dublin_postcode_residential_gas_consumption",
    "amalgamate_synthetic_ber_gas_consumption_to_postcodes",
]
product = None
# -

# manually create parent dir as ploomber isnt doing this
processed_dir = Path(product["csv"]).parent
processed_dir.mkdir(exist_ok=True)

cso_gas = pd.read_csv(
    upstream["create_dublin_postcode_residential_gas_consumption"], index_col=0
)

ber_gas = pd.read_csv(
    upstream["amalgamate_synthetic_ber_gas_consumption_to_postcodes"], index_col=0
)

ber_gas_vs_cso_gas = pd.concat([cso_gas, ber_gas], axis=1).dropna(how="any")

melted_ber_gas_vs_cso_gas = (
    ber_gas_vs_cso_gas.loc[:, ["2020", "ber_gas_consumption"]]
    .reset_index()
    .rename(
        columns={
            "index": "Postcodes",
            "2020": "CSO 2020",
            "ber_gas_consumption": "Synthetic BERs",
        }
    )
    .melt(id_vars="Postcodes", var_name="Source", value_name="GWh per year")
)

sns.catplot(
    data=melted_ber_gas_vs_cso_gas,
    x="Postcodes",
    y="GWh per year",
    hue="Source",
    kind="bar",
    height=10,
)
plt.xticks(rotation=45)

ber_gas_vs_cso_gas.to_csv(product["csv"])
