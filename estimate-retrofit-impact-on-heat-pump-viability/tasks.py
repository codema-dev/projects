from collections import defaultdict
import json
from typing import Any

import numpy as np
import pandas as pd
import yaml

from rcbm import fab
from rcbm import htuse
from rcbm import vent


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

    drop_columns = ["wall_uvalue", "roof_uvalue", "window_uvalue"] + [
        c for c in pre_retrofit.columns if "demand" in c
    ]
    post_retrofit = pd.concat(
        [
            pre_retrofit.drop(columns=drop_columns),
            retrofitted_wall_uvalues,
            retrofitted_roof_uvalues,
            retrofitted_window_uvalues,
        ],
        axis=1,
    )
    post_retrofit.to_csv(product, index=False)


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
    costs.to_csv(product, index=False)


def _calc_fabric_heat_loss_coefficient(buildings: pd.DataFrame) -> pd.Series:
    return fab.calculate_fabric_heat_loss_coefficient(
        roof_area=buildings["roof_area"],
        roof_uvalue=buildings["roof_uvalue"],
        wall_area=buildings["wall_area"],
        wall_uvalue=buildings["wall_uvalue"],
        floor_area=buildings["floor_area"],
        floor_uvalue=buildings["floor_uvalue"],
        window_area=buildings["window_area"],
        window_uvalue=buildings["window_uvalue"],
        door_area=buildings["door_area"],
        door_uvalue=buildings["door_uvalue"],
        thermal_bridging_factor=0.05,
    )


def _calc_annual_heat_loss(buildings: pd.DataFrame) -> pd.Series:

    heat_loss_coefficient = _calc_fabric_heat_loss_coefficient(buildings)

    # DEAP 4.2.2 default internal & external temperatures
    internal_temperatures = np.array(
        [
            17.72,
            17.73,
            17.85,
            17.95,
            18.15,
            18.35,
            18.50,
            18.48,
            18.33,
            18.11,
            17.88,
            17.77,
        ]
    )
    external_temperatures = np.array(
        [
            5.3,
            5.5,
            7.0,
            8.3,
            11.0,
            13.5,
            15.5,
            15.2,
            13.3,
            10.4,
            7.5,
            6.0,
        ]
    )

    return htuse.calculate_heat_loss_per_year(
        heat_loss_coefficient,
        internal_temperatures,
        external_temperatures,
        how="monthly",
    )


def estimate_retrofit_energy_saving(
    upstream: Any, product: Any, rebound_effect: float = 1
) -> None:

    pre_retrofit = pd.read_csv(upstream["download_buildings"])
    post_retrofit = pd.read_csv(upstream["implement_retrofit_measures"])

    pre_retrofit_annual_heat_loss = _calc_annual_heat_loss(pre_retrofit)
    post_retrofit_annual_heat_loss = _calc_annual_heat_loss(post_retrofit)

    annual_energy_saving = rebound_effect * (
        pre_retrofit_annual_heat_loss - post_retrofit_annual_heat_loss
    )

    heat_loss = pd.concat(
        [
            post_retrofit,
            annual_energy_saving.rename("annual_energy_saving_kwh"),
        ],
        axis=1,
    )
    heat_loss.to_csv(product, index=False)


def estimate_retrofit_ber_rating_improvement(upstream: Any, product: Any) -> None:

    pre_retrofit = pd.read_csv(upstream["download_buildings"])
    post_retrofit = pd.read_csv(upstream["implement_retrofit_measures"])
    energy_saving = pd.read_csv(upstream["estimate_retrofit_energy_saving"])

    use_columns = [
        "ground_floor_area",
        "first_floor_area",
        "second_floor_area",
        "third_floor_area",
    ]
    floor_area = pre_retrofit[use_columns].fillna(0).sum(axis=1)

    energy_rating_improvement = energy_saving["annual_energy_saving_kwh"] / floor_area
    post_retrofit_energy_value = (
        pre_retrofit["energy_value"] - energy_rating_improvement
    )

    energy_rating = pd.concat(
        [post_retrofit, post_retrofit_energy_value.rename("energy_value")],
        axis=1,
    )
    energy_rating.to_csv(product, index=False)


def calculate_heat_loss_indicator_improvement(upstream: Any, product: Any) -> None:

    buildings = pd.read_csv(upstream["implement_retrofit_measures"])

    building_volume = (
        buildings["ground_floor_area"] * buildings["ground_floor_height"]
        + buildings["first_floor_area"] * buildings["first_floor_height"]
        + buildings["second_floor_area"] * buildings["second_floor_height"]
        + buildings["third_floor_area"] * buildings["third_floor_height"]
    ).rename("building_volume")
    is_draught_lobby = buildings["is_draught_lobby"].map({"YES": True, "NO": False})
    structure_type = buildings["structure_type"].map(
        {
            "Please select                 ": "unknown",
            "Masonry                       ": "masonry",
            "Timber or Steel Frame         ": "timber_or_steel",
            "Insulated Conctete Form       ": "concrete",
        }
    )
    is_floor_suspended = buildings["is_floor_suspended"].map(
        {
            "No                            ": "none",
            "Yes (Sealed)                  ": "sealed",
            "Yes (Unsealed)                ": "unsealed",
        }
    )
    infiltration_rate = vent.calculate_infiltration_rate(
        no_sides_sheltered=buildings["number_of_sides_sheltered"],
        building_volume=building_volume,
        no_chimneys=buildings["number_of_chimneys"],
        no_open_flues=buildings["number_of_open_flues"],
        no_fans=buildings["number_of_fans"],
        no_room_heaters=buildings["number_of_room_heaters"],
        is_draught_lobby=is_draught_lobby,
        permeability_test_result=buildings["permeability_test_result"],
        no_storeys=buildings["number_of_storeys"],
        percentage_draught_stripped=buildings["percentage_draught_stripped"],
        is_floor_suspended=is_floor_suspended,
        structure_type=structure_type,
    )

    ventilation_method = buildings["ventilation_method"].map(
        {
            "Natural vent.": "natural_ventilation",
            "Bal.whole mech.vent no heat re": "mechanical_ventilation_no_heat_recovery",
            "Whole house extract vent.": "positive_input_ventilation_from_outside",
            "Bal.whole mech.vent heat recvr": "mechanical_ventilation_heat_recovery",
            "Pos input vent.- outside": "positive_input_ventilation_from_outside",
            "Pos input vent.- loft": "positive_input_ventilation_from_loft",
        }
    )
    effective_air_rate_change = vent.calculate_effective_air_rate_change(
        ventilation_method=ventilation_method,
        building_volume=building_volume,
        infiltration_rate=infiltration_rate,
        heat_exchanger_efficiency=buildings["heat_exchanger_efficiency"],
    )
    ventilation_heat_loss_coefficient = (
        vent.calculate_ventilation_heat_loss_coefficient(
            building_volume=building_volume,
            effective_air_rate_change=effective_air_rate_change,
            ventilation_heat_loss_constant=0.33,  # DEAP 4.2.2 default
        )
    )

    fabric_heat_loss_coefficient = _calc_fabric_heat_loss_coefficient(buildings)

    heat_loss_coefficient = (
        fabric_heat_loss_coefficient + ventilation_heat_loss_coefficient
    )

    use_columns = [
        "ground_floor_area",
        "first_floor_area",
        "second_floor_area",
        "third_floor_area",
    ]
    floor_area = buildings[use_columns].fillna(0).sum(axis=1)
    heat_loss_indicator = heat_loss_coefficient / floor_area

    hli = pd.concat(
        [
            buildings,
            heat_loss_indicator.rename("post_retrofit_heat_loss_indicator"),
        ],
        axis=1,
    )
    hli.to_csv(product, index=False)
