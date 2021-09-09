# %%
from collections import defaultdict
from pathlib import Path

import pandas as pd

import tasks

# %% tags=["parameters"]
DATA_DIR = Path("data")
ber_filepath = Path(
    "data/external/dublin_census_2016_filled_with_ber_public_14_05_2021.parquet"
)

# %%
bers = pd.read_parquet(ber_filepath)

# %%
# where target uvalues are taken from gov.ie 2021 Technical Guidance Document Table 5
defaults = {
    "wall": {
        "uvalue": {"target": 0.35, "threshold": 1},
        "cost": {"lower": 50, "upper": 300},
        "typical_area": 70,
    },
    "roof": {
        "uvalue": {"target": 0.25, "threshold": 1},
        "cost": {"lower": 5, "upper": 30},
        "typical_area": 50,
    },
    "window": {
        "uvalue": {"target": 1.4, "threshold": 2},
        "cost": {"lower": 30, "upper": 150},
        "typical_area": 16,
    },
}


# %%
total_floor_area = (
    bers["ground_floor_area"]
    + bers["first_floor_area"]
    + bers["second_floor_area"]
    + bers["third_floor_area"]
)

# %%
post_retrofit_columns = [
    "door_area",
    "floor_area",
    "roof_area",
    "small_area",
    "wall_area",
    "window_area",
    "floor_uvalue",
    "door_uvalue",
]

# %%
pre_retrofit = bers
post_retrofit = bers[post_retrofit_columns].copy()

# %%
for component, properties in defaults.items():
    uvalue_column_name = component + "_uvalue"
    uvalues = pre_retrofit[uvalue_column_name].copy()
    where_uvalue_is_viable = (
        (uvalues > properties["uvalue"]["threshold"])
        & (pre_retrofit["heat_loss_parameter"] > 2)
        & (pre_retrofit["period_built"] != "PRE19")
    )
    uvalues.loc[where_uvalue_is_viable] = properties["uvalue"]["target"]
    post_retrofit[uvalue_column_name] = uvalues

# %%
pre_retrofit_fabric_heat_loss_w_per_k = tasks.calculate_fabric_heat_loss_w_per_k(
    pre_retrofit
)

# %%
post_retrofit_fabric_heat_loss_w_per_k = tasks.calculate_fabric_heat_loss_w_per_k(
    post_retrofit
)

# %%
pre_retrofit_fabric_heat_loss_kwh_per_year = tasks.htuse.calculate_heat_loss_per_year(
    pre_retrofit_fabric_heat_loss_w_per_k
)

# %%
post_retrofit_fabric_heat_loss_kwh_per_year = tasks.htuse.calculate_heat_loss_per_year(
    post_retrofit_fabric_heat_loss_w_per_k
)

# %%
energy_savings_kwh_per_y = (
    pre_retrofit_fabric_heat_loss_kwh_per_year
    - post_retrofit_fabric_heat_loss_kwh_per_year
)

# %%
small_area_energy_savings_mwh_per_y = (
    pd.concat([pre_retrofit["small_area"], energy_savings_kwh_per_y], axis=1)
    .groupby("small_area")
    .sum()
    .divide(1e3)
    .squeeze()
    .rename("heat_demand_savings_mwh_per_y")
)

# %%
small_area_energy_savings_mwh_per_y.to_csv(
    "data/processed/small_area_energy_savings.csv.gz"
)

# %%
