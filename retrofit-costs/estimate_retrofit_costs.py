# %%
from collections import defaultdict
from pathlib import Path

import pandas as pd

import tasks

# %% tags=["parameters"]
DATA_DIR = Path("data")
ber_filepath = Path("data/external/small_area_bers.parquet")

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
dict_of_costs = defaultdict(list)
for component, properties in defaults.items():
    uvalue_column_name = component + "_uvalue"
    uvalues = pre_retrofit[uvalue_column_name].copy()
    where_uvalue_is_viable = (uvalues > properties["uvalue"]["threshold"]) & (
        pre_retrofit["heat_loss_parameter"] > 2
    )
    uvalues.loc[where_uvalue_is_viable] = properties["uvalue"]["target"]
    post_retrofit[uvalue_column_name] = uvalues

    area_column_name = component + "_area"
    areas = pre_retrofit[area_column_name].copy()
    cost_lower = tasks.estimate_cost_of_fabric_retrofits(
        is_selected=where_uvalue_is_viable,
        cost=properties["cost"]["lower"],
        areas=areas,
    )
    cost_upper = tasks.estimate_cost_of_fabric_retrofits(
        is_selected=where_uvalue_is_viable,
        cost=properties["cost"]["upper"],
        areas=areas,
    )
    dict_of_costs[component + "_cost_lower"] = cost_lower
    dict_of_costs[component + "_cost_upper"] = cost_upper

retrofit_costs = pd.DataFrame(dict_of_costs)

# %%
retrofit_costs["small_area"] = pre_retrofit["small_area"]

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
small_area_total = retrofit_costs.groupby("small_area").sum().divide(1e6)

# %%
small_area_total.to_csv(Path(DATA_DIR) / "processed" / "small_area_retrofit_cost.csv")

# %%
