# %%
from pathlib import Path

from berpublicsearch.download import download_berpublicsearch_parquet
import dask.dataframe as dd

data_dir = Path("../data")

# %% [markdown]
# # Get BER Public Search Database

# %%
path_to_berpublicsearch = data_dir / "BERPublicsearch_parquet"

# %%
if not path_to_berpublicsearch.exists():
    download_berpublicsearch_parquet(
        email_address="rowan.molony@codema.ie",
        savedir=data_dir,
    )

# %%
ber_public = dd.read_parquet(path_to_berpublicsearch)

# %%
ber_public_dublin = ber_public[
    ber_public["CountyName"].str.contains("Dublin")
].compute()

# %%
ber_public_dublin.to_parquet(data_dir / "BERPublicsearch_Dublin.parquet")

# %%