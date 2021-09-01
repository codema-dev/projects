import json
import os
from pathlib import Path
from typing import Any
from typing import Dict

import fs
from fs.tools import copy_file_data
import jupytext
from jupytext import kernels
from jupytext import header
import pandas as pd
from prefect.tasks.jupyter import ExecuteNotebook
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


def check_if_s3_keys_are_defined() -> None:
    message = f"""

        Please create a .env file
        
        In this directory: {Path.cwd().resolve()}

        With the following contents:
        
        AWS_ACCESS_KEY_ID=YOUR_KEY
        AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
    """
    assert os.getenv("AWS_ACCESS_KEY_ID") is not None, message
    assert os.getenv("AWS_SECRET_ACCESS_KEY") is not None, message


def fetch_s3_file(bucket: str, filename: str, savedir: Path) -> None:
    savepath = savedir / filename
    if not savepath.exists():
        s3fs = fs.open_fs(bucket)
        with s3fs.open(filename, "rb") as remote_file:
            with open(savedir / filename, "wb") as local_file:
                copy_file_data(remote_file, local_file)


def _convert_py_to_ipynb(
    input_filepath: Path,
    output_filepath: Path,
) -> None:
    notebook = jupytext.read(input_filepath)
    notebook["metadata"]["kernelspec"] = kernels.kernelspec_from_language("python")
    jupytext.write(notebook, output_filepath, fmt="py:percent")


def execute_python_file(
    py_filepath: Path,
    ipynb_filepath: Path,
    parameters: Dict[str, Any],
) -> None:
    _convert_py_to_ipynb(py_filepath, ipynb_filepath)
    exe = ExecuteNotebook()
    exe.run(path=ipynb_filepath, parameters=parameters)


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
