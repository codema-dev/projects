from collections import defaultdict
import json
from pathlib import Path
from typing import Any

import fs
from fs.tools import copy_file_data
import numpy as np
import pandas as pd

from rcbm import fab
from rcbm import htuse


def get_data(filepath: str) -> Path:
    return Path(__name__).parent / filepath


def create_folder_structure(data_dirpath: Path) -> None:
    data_dirpath.mkdir(exist_ok=True)
    external_dir = data_dirpath / "external"
    external_dir.mkdir(exist_ok=True)
    interim_dir = data_dirpath / "interim"
    interim_dir.mkdir(exist_ok=True)
    processed_dir = data_dirpath / "processed"
    processed_dir.mkdir(exist_ok=True)


def fetch_s3_file(bucket: str, filename: str, savedir: Path) -> None:
    savepath = savedir / filename
    if not savepath.exists():
        s3fs = fs.open_fs(bucket)
        with s3fs.open(filename, "rb") as remote_file:
            with open(savedir / filename, "wb") as local_file:
                copy_file_data(remote_file, local_file)


def estimate_cost_of_fabric_retrofits(
    is_selected: pd.Series,
    cost: float,
    areas: pd.Series,
) -> pd.Series:
    return pd.Series([cost] * is_selected * areas, dtype="int64")


def calculate_fabric_heat_loss_w_per_k(buildings: pd.DataFrame) -> pd.Series:
    return fab.calculate_fabric_heat_loss(
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


def get_ber_rating(energy_values: pd.Series) -> pd.Series:
    return (
        pd.cut(
            energy_values,
            [
                -np.inf,
                25,
                50,
                75,
                100,
                125,
                150,
                175,
                200,
                225,
                260,
                300,
                340,
                380,
                450,
                np.inf,
            ],
            labels=[
                "A1",
                "A2",
                "A3",
                "B1",
                "B2",
                "B3",
                "C1",
                "C2",
                "C3",
                "D1",
                "D2",
                "E1",
                "E2",
                "F",
                "G",
            ],
        )
        .rename("energy_rating")
        .astype("string")
    )  # streamlit & altair don't recognise category


def load_defaults(product: Any) -> None:
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
    with open(product, "w") as f:
        json.dump(defaults, f)


def replace_uvalues_with_target_uvalues(upstream: Any, product: Any) -> None:

    with open(upstream["load_defaults"], "r") as f:
        defaults = json.load(f)
    pre_retrofit = pd.read_parquet(upstream["download_buildings"])

    post_retrofit_columns = [
        "small_area",
        "dwelling_type",
        "year_of_construction",
        "period_built",
        "archetype",
        "door_area",
        "floor_area",
        "roof_area",
        "small_area",
        "wall_area",
        "window_area",
        "floor_uvalue",
        "door_uvalue",
    ]
    post_retrofit = pre_retrofit[post_retrofit_columns].copy()

    for component, properties in defaults.items():
        uvalue_column = component + "_uvalue"
        is_retrofitted_column = component + "_is_retrofitted"
        uvalues = pre_retrofit[uvalue_column].copy()
        where_uvalue_is_viable = (
            (uvalues > properties["uvalue"]["threshold"])
            & (pre_retrofit["heat_loss_parameter"] > 2)
            & (pre_retrofit["period_built"] != "PRE19")
        )
        uvalues.loc[where_uvalue_is_viable] = properties["uvalue"]["target"]
        post_retrofit[uvalue_column] = uvalues
        post_retrofit[is_retrofitted_column] = where_uvalue_is_viable

    post_retrofit.to_csv(product)


def estimate_individual_building_retrofit_costs(upstream: Any, product: Any) -> None:

    with open(upstream["load_defaults"], "r") as f:
        defaults = json.load(f)
    pre_retrofit = pd.read_parquet(upstream["download_buildings"])

    dict_of_costs = defaultdict(list)
    for component, properties in defaults.items():

        uvalue_column = component + "_uvalue"
        is_retrofitted_column = component + "_is_retrofitted"

        uvalues = pre_retrofit[uvalue_column]
        where_uvalue_is_viable = (
            (uvalues > properties["uvalue"]["threshold"])
            & (pre_retrofit["heat_loss_parameter"] > 2)
            & (pre_retrofit["period_built"] != "PRE19")
        )
        dict_of_costs[is_retrofitted_column] = where_uvalue_is_viable

        area_column_name = component + "_area"
        areas = pre_retrofit[area_column_name].copy()
        dict_of_costs[component + "_cost_lower"] = pd.Series(
            [properties["cost"]["lower"]] * where_uvalue_is_viable * areas,
            dtype="int64",
        )
        dict_of_costs[component + "_cost_upper"] = pd.Series(
            [properties["cost"]["upper"]] * where_uvalue_is_viable * areas,
            dtype="int64",
        )

    dict_of_costs["is_pre1919"] = pre_retrofit["period_built"] == "PRE19"

    use_columns = [
        "small_area",
        "dwelling_type",
        "year_of_construction",
        "period_built",
        "archetype",
    ]
    retrofit_costs = pd.concat(
        [pre_retrofit[use_columns], pd.DataFrame(dict_of_costs)], axis=1
    )
    retrofit_costs.to_csv(product)


def estimate_small_area_retrofit_costs(upstream: Any, product: Any) -> None:
    retrofit_costs = pd.read_csv(
        upstream["estimate_individual_building_retrofit_costs"], index_col=0
    )

    small_area_retrofit_costs = (
        retrofit_costs.groupby("small_area").sum().drop(columns="year_of_construction")
    )
    small_area_buildings = (
        retrofit_costs.groupby("small_area").size().rename("number_of_buildings")
    )

    small_area_statistics = pd.concat(
        [
            small_area_retrofit_costs,
            small_area_buildings,
        ],
        axis=1,
    )
    small_area_statistics.to_csv(product)


def estimate_individual_building_retrofit_energy_saving(
    upstream: Any, product: Any
) -> None:

    pre_retrofit = pd.read_parquet(upstream["download_buildings"])
    post_retrofit = pd.read_csv(
        upstream["replace_uvalues_with_target_uvalues"], index_col=0
    )

    pre_retrofit_fabric_heat_loss_w_per_k = pre_retrofit.pipe(
        calculate_fabric_heat_loss_w_per_k
    ).rename("pre_retrofit_fabric_heat_loss_w_per_k")
    post_retrofit_fabric_heat_loss_w_per_k = post_retrofit.pipe(
        calculate_fabric_heat_loss_w_per_k
    ).rename("post_retrofit_fabric_heat_loss_w_per_k")
    pre_retrofit_fabric_heat_loss_kwh_per_year = (
        pre_retrofit_fabric_heat_loss_w_per_k.pipe(htuse.calculate_heat_loss_per_year)
    ).rename("pre_retrofit_fabric_heat_loss_kwh_per_year")
    post_retrofit_fabric_heat_loss_kwh_per_year = (
        post_retrofit_fabric_heat_loss_w_per_k.pipe(htuse.calculate_heat_loss_per_year)
    ).rename("post_retrofit_fabric_heat_loss_kwh_per_year")
    energy_saving_kwh_per_y = pre_retrofit_fabric_heat_loss_kwh_per_year.subtract(
        post_retrofit_fabric_heat_loss_kwh_per_year
    ).rename("energy_saving_kwh_per_y")

    use_columns = [
        "small_area",
        "dwelling_type",
        "year_of_construction",
        "period_built",
        "archetype",
    ] + [c for c in pre_retrofit.columns if "uvalue" in c]
    statistics = pd.concat(
        [
            pre_retrofit[use_columns].rename(
                columns=lambda c: "pre_retrofit_" + c if "uvalue" in c else c
            ),
            pre_retrofit_fabric_heat_loss_w_per_k,
            post_retrofit_fabric_heat_loss_w_per_k,
            pre_retrofit_fabric_heat_loss_kwh_per_year,
            post_retrofit_fabric_heat_loss_kwh_per_year,
            energy_saving_kwh_per_y,
        ],
        axis=1,
    )
    statistics.to_csv(product)
