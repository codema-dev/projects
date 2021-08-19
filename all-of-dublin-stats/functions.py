from pathlib import Path
from typing import Dict
from typing import List
from urllib.request import urlretrieve

import fsspec


import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set()


def check_file_exists(filepath: Path):
    assert filepath.exists(), f"Please download & unzip {filepath} to data/external/"


def create_folder_structure(data_dirpath: Path) -> None:
    data_dirpath.mkdir(exist_ok=True)
    external_dir = data_dirpath / "external"
    external_dir.mkdir(exist_ok=True)
    interim_dir = data_dirpath / "interim"
    interim_dir.mkdir(exist_ok=True)
    processed_dir = data_dirpath / "processed"
    processed_dir.mkdir(exist_ok=True)


def load_parquet(url: str, filepath: Path) -> pd.DataFrame:
    if filepath.exists():
        df = pd.read_parquet(filepath)
    else:
        with fsspec.open(url) as f:
            df = pd.read_parquet(f)
        df.to_parquet(filepath)
    return df


def remove_rows(df: pd.DataFrame, column: str, values: List[str]) -> pd.DataFrame:
    rows_corresponding_to_values = df[column].isin(values)
    return df[~rows_corresponding_to_values].copy()


def estimate_residential_emissions(
    bers: pd.DataFrame,
    electricity_mwh: float,
    fuels_to_emission_factors: Dict[str, float],
    heating_columns: List[str],
) -> pd.Series:
    emission_factors = bers["main_sh_boiler_fuel"].map(fuels_to_emission_factors)
    heating_emissions = bers[heating_columns].sum(axis=1).multiply(emission_factors)
    return (
        heating_emissions.sum()
        + electricity_mwh * fuels_to_emission_factors["Electricity"]
    )


def plot_pie(s: pd.Series, filepath: Path) -> None:
    ax = s.plot.pie()
    fig = ax.get_figure()
    fig.savefig(filepath)
    plt.close(fig)
