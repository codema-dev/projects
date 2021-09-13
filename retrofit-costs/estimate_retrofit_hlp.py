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
defaults = {
    "wall": {
        "uvalue": {"target": 0.2, "threshold": 0.5},
        "cost": {"lower": 50, "upper": 300},
        "typical_area": 70,
    },
    "roof": {
        "uvalue": {"target": 0.13, "threshold": 0.5},
        "cost": {"lower": 5, "upper": 30},
        "typical_area": 50,
    },
    "window": {
        "uvalue": {"target": 0.2, "threshold": 0.5},
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
    where_uvalue_is_viable = (uvalues > properties["uvalue"]["threshold"]) & (
        pre_retrofit["heat_loss_parameter"] > 2
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
heat_loss_parameter_improvement = (
    pre_retrofit_fabric_heat_loss_w_per_k - post_retrofit_fabric_heat_loss_w_per_k
) / total_floor_area

# %%
post_retrofit_heat_loss_parameter = (
    pre_retrofit["heat_loss_parameter"] - heat_loss_parameter_improvement
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
pre_retrofit_fabric_heat_loss_kwh_per_year = tasks.htuse.calculate_heat_loss_per_year(
    pre_retrofit_fabric_heat_loss_w_per_k
)

# %%
post_retrofit_fabric_heat_loss_kwh_per_year = tasks.htuse.calculate_heat_loss_per_year(
    post_retrofit_fabric_heat_loss_w_per_k
)

# %%
energy_rating_improvement = (
    pre_retrofit_fabric_heat_loss_kwh_per_year
    - post_retrofit_fabric_heat_loss_kwh_per_year
) / total_floor_area

# %%
post_retrofit_energy_value = pre_retrofit["energy_value"] - energy_rating_improvement

# %%
post_retrofit_energy_rating = tasks.get_ber_rating(post_retrofit_energy_value)

# %%
