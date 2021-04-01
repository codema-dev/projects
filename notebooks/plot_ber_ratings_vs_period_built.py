# %%
from pathlib import Path

import pandas as pd
import numpy as np

import pandas_bokeh

pandas_bokeh.output_notebook()
data_dir = Path("../data")
html_dir = Path("../html")

# %% [markdown]
# # Get Dublin BER Public

# %%
keep_columns = [
    "CountyName",
    "Year_of_Construction",
    "EnergyRating",
]
ber_dublin = pd.read_parquet(data_dir / "BERPublicsearch_Dublin.parquet").assign(
    cso_period_built=lambda df: pd.cut(
        df["Year_of_Construction"],
        bins=[-np.inf, 1919, 1945, 1960, 1970, 1980, 1990, 2000, 2010, np.inf],
        labels=[
            "before 1919",
            "1919 - 1945",
            "1946 - 1960",
            "1961 - 1970",
            "1971 - 1980",
            "1981 - 1990",
            "1991 - 2000",
            "2001 - 2010",
            "2011 or later",
        ],
    ),
    BerBand=lambda df: df["EnergyRating"].str[0],
    EstimatedBer=lambda df: df["cso_period_built"].replace(
        {
            "before 1919": "E",
            "1919 - 1945": "E",
            "1946 - 1960": "E",
            "1961 - 1970": "D",
            "1971 - 1980": "D",
            "1981 - 1990": "D",
            "1991 - 2000": "D",
            "2001 - 2010": "C",
            "2011 or later": "A",
        }
    ),
)

# %% [markdown]
# # Plot BER Ratings vs Age

# %%
pandas_bokeh.output_file(html_dir / "ber_ratings_vs_period_built.html")

# %%
ber_dublin.pivot_table(
    index="EnergyRating",
    columns="cso_period_built",
    values="Year_of_Construction",
    aggfunc="count",
).plot_bokeh.bar(ylabel="Number of Buildings")

# %% [markdown]
# # Plot BER Bands vs Age

# %%
pandas_bokeh.output_file(html_dir / "ber_bands_vs_period_built.html")
# %%
ber_dublin.pivot_table(
    index="BerBand",
    columns="cso_period_built",
    values="Year_of_Construction",
    aggfunc="count",
).plot_bokeh.bar(ylabel="Number of Buildings")

# %% [markdown]
# # Plot Estimated BER Ratings vs Age

# %%
pandas_bokeh.output_file(html_dir / "ber_estimated_vs_period_built.html")

# %%
ber_dublin.pivot_table(
    index="EstimatedBer",
    columns="cso_period_built",
    values="Year_of_Construction",
    aggfunc="count",
).plot_bokeh.bar(ylabel="Number of Buildings")

# %%
