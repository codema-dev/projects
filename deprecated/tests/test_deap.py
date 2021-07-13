import numpy as np
import pandas as pd
from pandas.testing import assert_series_equal
import pytest


from dublin_building_stock.deap import calculate_building_volume
from dublin_building_stock.deap import calculate_fabric_heat_loss
from dublin_building_stock.deap import calculate_heat_loss_parameter
from dublin_building_stock.deap import calculate_ventilation_heat_loss


@pytest.fixture
def building_fabric():
    floor_uvalue = pd.Series([0.14])
    roof_uvalue = pd.Series([0.11])
    wall_uvalue = pd.Series([0.13])
    window_uvalue = pd.Series([0.87])
    door_uvalue = pd.Series([1.5])
    thermal_bridging_factor = pd.Series([0.05])
    effective_air_rate_change = pd.Series([0.5])

    return (
        floor_uvalue,
        roof_uvalue,
        wall_uvalue,
        window_uvalue,
        door_uvalue,
        thermal_bridging_factor,
        effective_air_rate_change,
    )


@pytest.fixture
def building_area():
    floor_area = pd.Series([63])
    roof_area = pd.Series([63])
    wall_area = pd.Series([85.7])
    window_area = pd.Series([29.6])
    door_area = pd.Series([1.85])

    return floor_area, roof_area, wall_area, window_area, door_area


@pytest.fixture
def building_floor_dimensions():
    ground_floor_area = pd.Series([63])
    ground_floor_height = pd.Series([2.4])
    first_floor_area = pd.Series([63])
    first_floor_height = pd.Series([2.7])
    second_floor_area = pd.Series([np.nan])
    second_floor_height = pd.Series([np.nan])
    third_floor_area = pd.Series([np.nan])
    third_floor_height = pd.Series([np.nan])

    return (
        ground_floor_area,
        ground_floor_height,
        first_floor_area,
        first_floor_height,
        second_floor_area,
        second_floor_height,
        third_floor_area,
        third_floor_height,
    )


@pytest.fixture
def building_volume(building_floor_dimensions):
    (
        ground_floor_area,
        ground_floor_height,
        first_floor_area,
        first_floor_height,
        second_floor_area,
        second_floor_height,
        third_floor_area,
        third_floor_height,
    ) = building_floor_dimensions

    return calculate_building_volume(
        ground_floor_area=ground_floor_area,
        ground_floor_height=ground_floor_height,
        first_floor_area=first_floor_area,
        first_floor_height=first_floor_height,
        second_floor_area=second_floor_area,
        second_floor_height=second_floor_height,
        third_floor_area=third_floor_area,
        third_floor_height=third_floor_height,
    )


@pytest.fixture
def building_floor_dimensions_approx():

    no_of_storeys = pd.Series([2])
    assumed_floor_height = pd.Series([2.5])
    return no_of_storeys, assumed_floor_height


def test_calculate_fabric_heat_loss(building_area, building_fabric):
    """Output is equivalent to DEAP 4.2.0 example A"""
    floor_area, roof_area, wall_area, window_area, door_area = building_area
    (
        floor_uvalue,
        roof_uvalue,
        wall_uvalue,
        window_uvalue,
        door_uvalue,
        thermal_bridging_factor,
        *_,
    ) = building_fabric

    expected_output = pd.Series([68], dtype="int64")

    output = calculate_fabric_heat_loss(
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
    rounded_output = output.round().astype("int64")

    assert_series_equal(rounded_output, expected_output)


def test_calculate_building_volume(building_floor_dimensions, building_volume):
    """Output is equivalent to DEAP 4.2.0 example A"""
    (
        ground_floor_area,
        ground_floor_height,
        first_floor_area,
        first_floor_height,
        second_floor_area,
        second_floor_height,
        third_floor_area,
        third_floor_height,
    ) = building_floor_dimensions

    expected_output = pd.Series([321], dtype="int64")

    output = building_volume
    rounded_output = output.round().astype("int64")

    assert_series_equal(rounded_output, expected_output)


def test_calculate_ventilation_heat_loss(building_volume, building_fabric):
    """Output is equivalent to DEAP 4.2.0 example A"""
    effective_air_rate_change = building_fabric[-1]

    expected_output = pd.Series([53], dtype="int64")

    output = calculate_ventilation_heat_loss(
        building_volume=building_volume,
        effective_air_rate_change=effective_air_rate_change,
    )
    rounded_output = output.round().astype("int64")

    assert_series_equal(rounded_output, expected_output)


def test_calculate_heat_loss_parameter(
    building_area,
    building_fabric,
    building_floor_dimensions,
    building_floor_dimensions_approx,
):
    """Output is equivalent to DEAP 4.2.0 example A"""
    floor_area, roof_area, wall_area, window_area, door_area = building_area
    (
        floor_uvalue,
        roof_uvalue,
        wall_uvalue,
        window_uvalue,
        door_uvalue,
        thermal_bridging_factor,
        effective_air_rate_change,
    ) = building_fabric
    (
        ground_floor_area,
        ground_floor_height,
        first_floor_area,
        first_floor_height,
        second_floor_area,
        second_floor_height,
        third_floor_area,
        third_floor_height,
    ) = building_floor_dimensions
    total_floor_area = (
        ground_floor_area
        + first_floor_area.fillna(0)
        + second_floor_area.fillna(0)
        + third_floor_area.fillna(0)
    )
    no_of_storeys, assumed_floor_height = building_floor_dimensions_approx

    expected_output = pd.Series([0.96], dtype="float64")

    output = calculate_heat_loss_parameter(
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
        total_floor_area=total_floor_area,
        thermal_bridging_factor=thermal_bridging_factor,
        effective_air_rate_change=effective_air_rate_change,
        ground_floor_area=ground_floor_area,
        ground_floor_height=ground_floor_height,
        first_floor_area=first_floor_area,
        first_floor_height=first_floor_height,
        second_floor_area=second_floor_area,
        second_floor_height=second_floor_height,
        third_floor_area=third_floor_area,
        third_floor_height=third_floor_height,
        no_of_storeys=no_of_storeys,
        assumed_floor_height=assumed_floor_height,
    )
    rounded_output = output.round(2).astype("float64")

    assert_series_equal(rounded_output, expected_output)
