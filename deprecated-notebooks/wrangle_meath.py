from pathlib import Path

import dask.dataframe as dd
import geopandas as gpd
import pandas as pd
import numpy as np

data_dir = Path("../data")

# %%
meath_small_area_boundaries = gpd.read_file(
    data_dir
    / "Small_Areas_Ungeneralised_-_OSi_National_Statistical_Boundaries_-_2015-shp"
).query("COUNTYNAME == 'Meath'")[["SMALL_AREA", "EDNAME", "geometry"]]

# %%
meath_census_2016 = (
    pd.read_csv(data_dir / "SAPS2016_SA2017.csv")
    .assign(SMALL_AREA=lambda gdf: gdf["GEOGID"].str[7:])
    .merge(meath_small_area_boundaries_2016, on="SMALL_AREA")
)

# %%
filepath = data_dir / "meath_ber_public.csv"
if not filepath.exists():
    meath_ber_public = (
        dd.read_parquet(data_dir / "BERPublicsearch_parquet")
        .query("CountyName == 'Co. Meath'")
        .compute()
    )
    meath_ber_public.to_csv(filepath)

meath_ber_public = pd.read_csv(filepath, low_memory=False).assign(
    BERBand=lambda df: df["EnergyRating"].str[0].astype("category"),
    period_built=lambda df: pd.cut(
        df["Year_of_Construction"],
        bins=[
            -np.inf,
            1919,
            1945,
            1960,
            1970,
            1980,
            1990,
            2000,
            2005,
            2011,
            np.inf,
        ],
        labels=[
            "before 1919",
            "1919 - 1945",
            "1946 - 1960",
            "1961 - 1970",
            "1971 - 1980",
            "1981 - 1990",
            "1991 - 2000",
            "2001 - 2005",
            "2006 - 2010",
            "2011 or later",
        ],
    ),
    dwelling_type=lambda df: df["DwellingTypeDescr"]
    .replace(
        {
            "Ground-floor apartment": "Apartment",
            "Mid-floor apartment": "Apartment",
            "Top-floor apartment": "Apartment",
            "Basement Dwelling": "Apartment",
            "Maisonette": "Apartment",
            "End of terrace house": "Terraced house",
            "Mid-terrace house": "Terraced house",
            "House": "Detached house",
            None: "Not stated",
        }
    )
    .astype("category"),
    total_floor_area=lambda df: df["GroundFloorArea"]
    + df["FirstFloorArea"]
    + df["SecondFloorArea"]
    + df["ThirdFloorArea"],
    DeliveredEnergyBoilers=lambda df: df[
        [
            "DeliveredEnergyMainWater",
            "DeliveredEnergyMainSpace",
            "DeliveredEnergySecondarySpace",
            "DeliveredEnergySupplementaryWater",
        ]
    ].sum(axis=1),
)

# %%
meath_ber_public_extract = meath_ber_public[
    [
        "BERBand",
        "dwelling_type",
        "period_built",
        "total_floor_area",
        "MainSpaceHeatingFuel",
        "MainWaterHeatingFuel",
        "SupplSHFuel",
        "SupplWHFuel",
        "HSMainSystemEfficiency",
        "WHMainSystemEff",
        "HSSupplSystemEff",
        "DeliveredEnergyMainWater",
        "DeliveredEnergyMainSpace",
        "DeliveredEnergySecondarySpace",
        "DeliveredEnergySupplementaryWater",
        "DeliveredEnergyBoilers",
        "DeliveredLightingEnergy",
        "DeliveredEnergyPumpsFans",
        "TotalDeliveredEnergy",
    ]
]

# %%
meath_ber_public_extract.to_csv(data_dir / "meath_ber_public_extract.csv", index=False)

# %%
mode_ber_bands = (
    meath_ber_public_extract.groupby(["dwelling_type", "period_built"])["BERBand"]
    .agg(pd.Series.mode)
    .rename("ModeBERBand")
)

# %%
median_floor_areas = (
    meath_ber_public_extract.groupby(["dwelling_type", "period_built"])[
        "total_floor_area"
    ]
    .median()
    .rename("MedianTotalFloorArea")
)

# %%
mode_fuel = (
    meath_ber_public_extract.groupby(["dwelling_type", "period_built"])[
        "MainSpaceHeatingFuel"
    ]
    .agg(pd.Series.mode)
    .rename("ModeMainSpaceHeatingFuel")
)

# %%
median_boiler_efficiencies = (
    meath_ber_public_extract.groupby(["dwelling_type", "period_built"])[
        "HSMainSystemEfficiency"
    ]
    .median()
    .rename("MedianHSMainSystemEfficiency")
)

# %%
median_boiler_energies = (
    meath_ber_public_extract.groupby(["dwelling_type", "period_built"])[
        "DeliveredEnergyBoilers"
    ]
    .median()
    .rename("MedianDeliveredEnergyBoilers")
)

# %%
median_lighting_energies = (
    meath_ber_public_extract.groupby(["dwelling_type", "period_built"])[
        "DeliveredLightingEnergy"
    ]
    .median()
    .rename("MedianDeliveredLightingEnergy")
)

# %%
median_pumpsfans_energies = (
    meath_ber_public_extract.groupby(["dwelling_type", "period_built"])[
        "DeliveredEnergyPumpsFans"
    ]
    .median()
    .rename("MedianDeliveredEnergyPumpsFans")
)

# %%
median_total_energies = (
    meath_ber_public_extract.groupby(["dwelling_type", "period_built"])[
        "TotalDeliveredEnergy"
    ]
    .median()
    .rename("MedianTotalDeliveredEnergy")
)

# %%
archetypes = pd.concat(
    [
        mode_ber_bands,
        median_floor_areas,
        mode_fuel,
        median_boiler_efficiencies,
        median_boiler_energies,
        median_lighting_energies,
        median_pumpsfans_energies,
        median_total_energies,
    ],
    axis=1,
)
# %%
archetypes.to_csv(data_dir / "meath_ber_public_archetypes.csv")
# %%
