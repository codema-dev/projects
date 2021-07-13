import pandas as pd
import numpy as np


def calculate_fabric_heat_loss(
    roof_area,
    roof_uvalue,
    wall_area,
    wall_uvalue,
    floor_area,
    floor_uvalue,
    window_area,
    window_uvalue,
    door_area,
    door_uvalue,
    thermal_bridging_factor,
):
    plane_elements_area = roof_area + floor_area + door_area + wall_area + window_area
    thermal_bridging = thermal_bridging_factor * plane_elements_area
    heat_loss_via_plane_elements = (
        wall_area * wall_uvalue
        + roof_area * roof_uvalue
        + floor_area * floor_uvalue
        + window_area * window_uvalue
        + door_area * door_uvalue
    )

    return thermal_bridging + heat_loss_via_plane_elements


def calculate_building_volume(
    ground_floor_area=None,
    ground_floor_height=None,
    first_floor_area=None,
    first_floor_height=None,
    second_floor_area=None,
    second_floor_height=None,
    third_floor_area=None,
    third_floor_height=None,
    no_of_storeys=None,
    floor_area=None,
    assumed_floor_height=None,
):
    if ground_floor_area is not None:
        building_volume = (
            ground_floor_area * ground_floor_height
            + first_floor_area.fillna(0) * first_floor_height.fillna(0)
            + second_floor_area.fillna(0) * second_floor_height.fillna(0)
            + third_floor_area.fillna(0) * third_floor_height.fillna(0)
        )
    elif no_of_storeys is not None:
        building_volume = floor_area * no_of_storeys * assumed_floor_height
    else:
        raise ValueError(
            "Must specify either 'no_of_storeys'"
            "or floor areas & heights to calculate building volume!"
        )

    return building_volume


def calculate_ventilation_heat_loss(
    building_volume,
    effective_air_rate_change,
):
    ventilation_heat_loss_constant = 0.33  # SEAI, DEAP 4.2.0
    return building_volume * ventilation_heat_loss_constant * effective_air_rate_change


def calculate_heat_loss_parameter(
    roof_area,
    roof_uvalue,
    wall_area,
    wall_uvalue,
    floor_area,
    floor_uvalue,
    window_area,
    window_uvalue,
    door_area,
    door_uvalue,
    total_floor_area,
    thermal_bridging_factor,
    effective_air_rate_change,
    ground_floor_area=None,
    ground_floor_height=None,
    first_floor_area=None,
    first_floor_height=None,
    second_floor_area=None,
    second_floor_height=None,
    third_floor_area=None,
    third_floor_height=None,
    no_of_storeys=None,
    assumed_floor_height=2.5,
) -> pd.DataFrame:
    fabric_heat_loss = calculate_fabric_heat_loss(
        roof_area=roof_area,
        roof_uvalue=roof_uvalue,
        wall_area=wall_area,
        wall_uvalue=wall_uvalue,
        floor_area=floor_area,
        floor_uvalue=floor_uvalue,
        window_area=window_area,
        window_uvalue=window_uvalue,
        door_area=door_area,
        door_uvalue=door_uvalue,
        thermal_bridging_factor=thermal_bridging_factor,
    )
    building_volume = calculate_building_volume(
        ground_floor_area=ground_floor_area,
        ground_floor_height=ground_floor_height,
        first_floor_area=first_floor_area,
        first_floor_height=first_floor_height,
        second_floor_area=second_floor_area,
        second_floor_height=second_floor_height,
        third_floor_area=third_floor_area,
        third_floor_height=third_floor_height,
        no_of_storeys=no_of_storeys,
        floor_area=floor_area,
        assumed_floor_height=assumed_floor_height,
    )
    ventilation_heat_loss = calculate_ventilation_heat_loss(
        building_volume=building_volume,
        effective_air_rate_change=effective_air_rate_change,
    )
    heat_loss_coefficient = fabric_heat_loss + ventilation_heat_loss
    return heat_loss_coefficient / total_floor_area
