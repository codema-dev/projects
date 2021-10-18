import numpy as np
import pandas as pd

import seaborn as sns

sns.set()


def _band_energy_value_into_ratings(energy_values: pd.Series) -> pd.Series:
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


# + tags=["parameters"]
upstream = [
    "download_buildings",
    "estimate_retrofit_ber_rating_improvement",
]
product = None
# -

pre_retrofit = pd.read_csv(upstream["download_buildings"])

ber_improvement = pd.read_csv(upstream["estimate_retrofit_ber_rating_improvement"])

_band_energy_value_into_ratings(
    pre_retrofit["energy_value"]
).value_counts().sort_index().plot.bar()

_band_energy_value_into_ratings(
    ber_improvement["energy_value"]
).value_counts().sort_index().plot.bar()
