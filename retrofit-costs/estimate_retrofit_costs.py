# %%
from pathlib import Path

import pandas as pd

# %% tags=["parameters"]
DATA_DIR = Path("data")
ber_filepath = Path("data/external/small_area_bers.parquet")

# %%
bers = pd.read_parquet(ber_filepath)

# %%
