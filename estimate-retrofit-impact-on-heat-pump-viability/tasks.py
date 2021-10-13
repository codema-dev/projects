from collections import defaultdict
import json
from typing import Any

import numpy as np
import pandas as pd
import yaml

from rcbm import fab
from rcbm import htuse


def load_defaults(product: Any) -> None:
    yaml_defaults = """
    wall_uvalue:
      target: 0.35
      threshold: 1
    wall_cost:
      lower: 50
      upper: 300
    roof_uvalue:
      target: 0.25
      threshold: 1
    roof_cost:
      lower: 5
      upper: 30
    window_uvalue:
      target: 1.4
      threshold: 2
    window_cost:
      lower: 30
      upper: 150
    """
    defaults = yaml.safe_load(yaml_defaults)
    with open(product, "w") as f:
        json.dump(defaults, f)


def _replace_property_with_value(
    values,
    threshold,
    target_value,
) -> pd.Series:
    where_is_viable = values > threshold
    values.loc[where_is_viable] = target_value
    return values


def implement_retrofit_measures(upstream: Any, product: Any) -> None:

    with open(upstream["load_defaults"], "r") as f:
        defaults = json.load(f)
    pre_retrofit = pd.read_csv(upstream["download_buildings"])

    retrofitted_wall_uvalues = _replace_property_with_value(
        pre_retrofit["wall_uvalue"],
        defaults["wall_uvalue"]["threshold"],
        defaults["wall_uvalue"]["target"],
    )
    retrofitted_roof_uvalues = _replace_property_with_value(
        pre_retrofit["roof_uvalue"],
        defaults["roof_uvalue"]["threshold"],
        defaults["roof_uvalue"]["target"],
    )
    retrofitted_window_uvalues = _replace_property_with_value(
        pre_retrofit["window_uvalue"],
        defaults["window_uvalue"]["threshold"],
        defaults["window_uvalue"]["target"],
    )

    use_columns = [
        "small_area",
        "dwelling_type",
        "year_of_construction",
        "period_built",
        "archetype",
        "door_area",
        "floor_area",
        "roof_area",
        "wall_area",
        "window_area",
        "floor_uvalue",
        "door_uvalue",
    ]
    post_retrofit = pd.concat(
        [
            pre_retrofit[use_columns],
            retrofitted_wall_uvalues,
            retrofitted_roof_uvalues,
            retrofitted_window_uvalues,
        ],
        axis=1,
    )
    post_retrofit.to_csv(product)


def _estimate_component_cost(
    pre_retrofit,
    post_retrofit,
    target_value,
    areas,
    name,
) -> pd.Series:
    where_is_retrofitted = pre_retrofit != post_retrofit
    return pd.Series(
        [target_value] * where_is_retrofitted * areas, dtype="int64", name=name
    )


def estimate_retrofit_costs(upstream: Any, product: Any) -> None:

    with open(upstream["load_defaults"], "r") as f:
        defaults = json.load(f)
    pre_retrofit = pd.read_csv(upstream["download_buildings"])
    post_retrofit = pd.read_csv(upstream["implement_retrofit_measures"])

    wall_cost_lower = _estimate_component_cost(
        pre_retrofit["wall_uvalue"],
        post_retrofit["wall_uvalue"],
        defaults["wall_cost"]["lower"],
        pre_retrofit["wall_area"],
        "wall_cost_lower",
    )
    wall_cost_upper = _estimate_component_cost(
        pre_retrofit["wall_uvalue"],
        post_retrofit["wall_uvalue"],
        defaults["wall_cost"]["upper"],
        pre_retrofit["wall_area"],
        "wall_cost_upper",
    )

    roof_cost_lower = _estimate_component_cost(
        pre_retrofit["roof_uvalue"],
        post_retrofit["roof_uvalue"],
        defaults["roof_cost"]["lower"],
        pre_retrofit["roof_area"],
        "roof_cost_lower",
    )
    roof_cost_upper = _estimate_component_cost(
        pre_retrofit["roof_uvalue"],
        post_retrofit["roof_uvalue"],
        defaults["roof_cost"]["upper"],
        pre_retrofit["roof_area"],
        "roof_cost_upper",
    )

    window_cost_lower = _estimate_component_cost(
        pre_retrofit["window_uvalue"],
        post_retrofit["window_uvalue"],
        defaults["window_cost"]["lower"],
        pre_retrofit["window_area"],
        "window_cost_lower",
    )
    window_cost_upper = _estimate_component_cost(
        pre_retrofit["window_uvalue"],
        post_retrofit["window_uvalue"],
        defaults["window_cost"]["upper"],
        pre_retrofit["window_area"],
        "window_cost_upper",
    )

    costs = pd.concat(
        [
            wall_cost_lower,
            wall_cost_upper,
            roof_cost_lower,
            roof_cost_upper,
            window_cost_lower,
            window_cost_upper,
        ],
        axis=1,
    )
    costs.to_csv(product)


# def calculate_fabric_heat_loss_w_per_k(buildings: pd.DataFrame) -> pd.Series:
#     return fab.calculate_fabric_heat_loss(
#         roof_area=buildings["roof_area"],
#         roof_uvalue=buildings["roof_uvalue"],
#         wall_area=buildings["wall_area"],
#         wall_uvalue=buildings["wall_uvalue"],
#         floor_area=buildings["floor_area"],
#         floor_uvalue=buildings["floor_uvalue"],
#         window_area=buildings["window_area"],
#         window_uvalue=buildings["window_uvalue"],
#         door_area=buildings["door_area"],
#         door_uvalue=buildings["door_uvalue"],
#         thermal_bridging_factor=0.05,
#     )


# def get_ber_rating(energy_values: pd.Series) -> pd.Series:
#     return (
#         pd.cut(
#             energy_values,
#             [
#                 -np.inf,
#                 25,
#                 50,
#                 75,
#                 100,
#                 125,
#                 150,
#                 175,
#                 200,
#                 225,
#                 260,
#                 300,
#                 340,
#                 380,
#                 450,
#                 np.inf,
#             ],
#             labels=[
#                 "A1",
#                 "A2",
#                 "A3",
#                 "B1",
#                 "B2",
#                 "B3",
#                 "C1",
#                 "C2",
#                 "C3",
#                 "D1",
#                 "D2",
#                 "E1",
#                 "E2",
#                 "F",
#                 "G",
#             ],
#         )
#         .rename("energy_rating")
#         .astype("string")
#     )  # streamlit & altair don't recognise category


# def estimate_retrofit_energy_saving(
#     upstream: Any, product: Any, rebound_effect: float = 1
# ) -> None:

#     pre_retrofit = pd.read_parquet(upstream["download_buildings"])
#     post_retrofit = pd.read_csv(
#         upstream["replace_uvalues_with_target_uvalues"], index_col=0
#     )

#     pre_retrofit_fabric_heat_loss_w_per_k = pre_retrofit.pipe(
#         calculate_fabric_heat_loss_w_per_k
#     ).rename("pre_retrofit_fabric_heat_loss_w_per_k")
#     post_retrofit_fabric_heat_loss_w_per_k = post_retrofit.pipe(
#         calculate_fabric_heat_loss_w_per_k
#     ).rename("post_retrofit_fabric_heat_loss_w_per_k")
#     pre_retrofit_fabric_heat_loss_kwh_per_year = (
#         pre_retrofit_fabric_heat_loss_w_per_k.pipe(htuse.calculate_heat_loss_per_year)
#     ).rename("pre_retrofit_fabric_heat_loss_kwh_per_year")
#     post_retrofit_fabric_heat_loss_kwh_per_year = (
#         post_retrofit_fabric_heat_loss_w_per_k.pipe(htuse.calculate_heat_loss_per_year)
#     ).rename("post_retrofit_fabric_heat_loss_kwh_per_year")
#     energy_saving_kwh_per_y = (
#         pre_retrofit_fabric_heat_loss_kwh_per_year.subtract(
#             post_retrofit_fabric_heat_loss_kwh_per_year
#         )
#         .multiply(rebound_effect)
#         .rename("energy_saving_kwh_per_y")
#     )

#     use_columns = [
#         "small_area",
#         "dwelling_type",
#         "year_of_construction",
#         "period_built",
#         "archetype",
#         "main_sh_boiler_fuel",
#     ] + [c for c in pre_retrofit.columns if "uvalue" in c]
#     statistics = pd.concat(
#         [
#             pre_retrofit[use_columns].rename(
#                 columns=lambda c: "pre_retrofit_" + c if "uvalue" in c else c
#             ),
#             pre_retrofit_fabric_heat_loss_w_per_k,
#             post_retrofit_fabric_heat_loss_w_per_k,
#             pre_retrofit_fabric_heat_loss_kwh_per_year,
#             post_retrofit_fabric_heat_loss_kwh_per_year,
#             energy_saving_kwh_per_y,
#         ],
#         axis=1,
#     )
#     statistics.to_csv(product, index=False)


# def estimate_retrofit_ber_rating_improvement(upstream: Any, product: Any) -> None:

#     pre_retrofit = pd.read_parquet(upstream["download_buildings"])
#     post_retrofit = pd.read_csv(
#         upstream["replace_uvalues_with_target_uvalues"], index_col=0
#     )

#     pre_retrofit_fabric_heat_loss_w_per_k = pre_retrofit.pipe(
#         calculate_fabric_heat_loss_w_per_k
#     ).rename("pre_retrofit_fabric_heat_loss_w_per_k")
#     post_retrofit_fabric_heat_loss_w_per_k = post_retrofit.pipe(
#         calculate_fabric_heat_loss_w_per_k
#     ).rename("post_retrofit_fabric_heat_loss_w_per_k")
#     pre_retrofit_fabric_heat_loss_kwh_per_year = (
#         pre_retrofit_fabric_heat_loss_w_per_k.pipe(htuse.calculate_heat_loss_per_year)
#     ).rename("pre_retrofit_fabric_heat_loss_kwh_per_year")
#     post_retrofit_fabric_heat_loss_kwh_per_year = (
#         post_retrofit_fabric_heat_loss_w_per_k.pipe(htuse.calculate_heat_loss_per_year)
#     ).rename("post_retrofit_fabric_heat_loss_kwh_per_year")
#     energy_saving_kwh_per_y = pre_retrofit_fabric_heat_loss_kwh_per_year.subtract(
#         post_retrofit_fabric_heat_loss_kwh_per_year
#     ).rename("energy_saving_kwh_per_y")

#     total_floor_area = (
#         pre_retrofit[
#             [
#                 "ground_floor_area",
#                 "first_floor_area",
#                 "second_floor_area",
#                 "third_floor_area",
#             ]
#         ]
#         .fillna(0)
#         .sum(axis=1)
#     )
#     energy_rating_improvement = energy_saving_kwh_per_y / total_floor_area
#     post_retrofit_energy_rating = get_ber_rating(
#         pre_retrofit["energy_value"] - energy_rating_improvement
#     )

#     use_columns = [
#         "small_area",
#         "dwelling_type",
#         "year_of_construction",
#         "period_built",
#         "archetype",
#     ] + [c for c in pre_retrofit.columns if "uvalue" in c]
#     statistics = pd.concat(
#         [
#             pre_retrofit[use_columns].rename(
#                 columns=lambda c: "pre_retrofit_" + c if "uvalue" in c else c
#             ),
#             pre_retrofit_fabric_heat_loss_w_per_k,
#             post_retrofit_fabric_heat_loss_w_per_k,
#             pre_retrofit_fabric_heat_loss_kwh_per_year,
#             post_retrofit_fabric_heat_loss_kwh_per_year,
#             post_retrofit_energy_rating,
#         ],
#         axis=1,
#     )
#     statistics.to_csv(product, index=False)


# def estimate_retrofit_hlp_improvement(
#     upstream: Any, product: Any, rebound_effect: float = 1
# ) -> None:

#     pre_retrofit = pd.read_parquet(upstream["download_buildings"])
#     post_retrofit = pd.read_csv(
#         upstream["replace_uvalues_with_target_uvalues"], index_col=0
#     )

#     total_floor_area = (
#         pre_retrofit["ground_floor_area"]
#         + pre_retrofit["first_floor_area"]
#         + pre_retrofit["second_floor_area"]
#         + pre_retrofit["third_floor_area"]
#     ).rename("total_floor_area")

#     pre_retrofit_fabric_heat_loss_w_per_k = pre_retrofit.pipe(
#         calculate_fabric_heat_loss_w_per_k
#     ).rename("pre_retrofit_fabric_heat_loss_w_per_k")
#     post_retrofit_fabric_heat_loss_w_per_k = post_retrofit.pipe(
#         calculate_fabric_heat_loss_w_per_k
#     ).rename("post_retrofit_fabric_heat_loss_w_per_k")
#     heat_loss_parameter_improvement = pre_retrofit_fabric_heat_loss_w_per_k.subtract(
#         post_retrofit_fabric_heat_loss_w_per_k
#     ).divide(total_floor_area)
#     post_retrofit_heat_loss_parameter = (
#         pre_retrofit["heat_loss_parameter"]
#         .subtract(heat_loss_parameter_improvement)
#         .rename("post_retrofit_heat_loss_parameter")
#     )

#     use_columns = [
#         "small_area",
#         "dwelling_type",
#         "year_of_construction",
#         "period_built",
#         "archetype",
#         "heat_loss_parameter",
#     ] + [c for c in pre_retrofit.columns if "uvalue" in c]
#     statistics = pd.concat(
#         [
#             pre_retrofit[use_columns].rename(
#                 columns=lambda c: "pre_retrofit_" + c if "uvalue" in c else c
#             ),
#             pre_retrofit_fabric_heat_loss_w_per_k,
#             post_retrofit_fabric_heat_loss_w_per_k,
#             total_floor_area,
#             post_retrofit_heat_loss_parameter,
#         ],
#         axis=1,
#     )
#     statistics.to_csv(product, index=False)
