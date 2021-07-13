# %%
from pathlib import Path

import geopandas as gpd
import pandas as pd

data_dir = Path("../data")


def clean_seda(df):
    return (
        df.dropna(how="all")
        .dropna(how="all", axis=1)
        .assign(ID=lambda df: df["ID"].astype(str))
        .query("ID != 'Total'")
    )


def convert_to_geodataframe(df, x, y, crs):
    geometry = gpd.points_from_xy(df[x], df[y])
    return gpd.GeoDataFrame(df, geometry=geometry, crs=crs).drop(columns=[x, y])


# %% [markdown]
# # Get Dublin Boundary
dublin_boundary = gpd.read_file(data_dir / "dublin_boundary.geojson", driver="GeoJSON")

# %% [markdown]
# # Merge SEDA 2015 Commercial Stock

# %%
sdcc = (
    pd.read_excel(data_dir / "sdcc_commercial_buildings_2015.xlsx")
    .pipe(clean_seda)
    .pipe(convert_to_geodataframe, x="X_IG", y="Y_IG", crs="EPSG:29903")
    .to_crs(epsg=2157)
    .pipe(gpd.sjoin, dublin_boundary, op="within")
)

# %%
dcc_dlrcc_fcc = pd.concat(
    [
        pd.read_excel(data_dir / f"{la}_commercial_buildings_2015.xlsx")
        .pipe(clean_seda)
        .pipe(convert_to_geodataframe, x="X_ITM", y="Y_ITM", crs="EPSG:2157")
        .pipe(gpd.sjoin, dublin_boundary, op="within")
        for la in ["dcc", "dlrcc", "fcc"]
    ]
)

# %%
seda_2015 = pd.concat([dcc_dlrcc_fcc, sdcc]).reset_index(drop=True)

# %% [markdown]
# # Save
seda_2015.to_file(data_dir / "commercial_buildings_2015.geojson", driver="GeoJSON")
