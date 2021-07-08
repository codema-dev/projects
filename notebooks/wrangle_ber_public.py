from pathlib import Path

import dask.dataframe as dd
import geopandas as gpd
import numpy as np
import pandas as pd

data_dir = Path("../data")

dublin_postcode_boundaries = gpd.read_file(
    data_dir / "dublin_postcode_boundaries.geojson", driver="GeoJSON"
)

ber_public = dd.read_parquet(data_dir / "BERPublicsearch_parquet")

# 90% of GroundFloorHeight within (2.25, 2.75) in BER Public
assumed_floor_height = 2.5
heat_loss_parameter_cutoff = 2.3  # SEAI, Tech Advisor Role Heat Pumps 2020
dublin_ber_public = (
    ber_public[ber_public["CountyName"].str.contains("Dublin")]
    .compute()
    .assign(
        BERBand=lambda df: df["EnergyRating"].str[0],
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
        dwelling_type=lambda df: df["DwellingTypeDescr"].replace(
            {
                "Ground-floor apartment": "Apartment",
                "Mid-floor apartment": "Apartment",
                "Top-floor apartment": "Apartment",
                "Maisonette": "Apartment",
                "Detached house": "Detached house",
                "Semi-detached house": "Semi-detached house",
                "House": "Semi-detached house",
                "Mid-terrace house": "Terraced house",
                "End of terrace house": "Terraced house",
                "Basement Dwelling": "Apartment",
                None: "Not stated",
            }
        ),
        total_floor_area=lambda df: df["GroundFloorArea"]
        + df["FirstFloorArea"].fillna(0)
        + df["SecondFloorArea"].fillna(0)
        + df["ThirdFloorArea"].fillna(0),
        effective_air_rate_change=lambda df: df["VentilationMethod"]
        .replace(
            {
                "Natural vent.": 0.52,
                "Pos input vent.- loft": 0.51,
                "Pos input vent.- outside": 0.5,
                "Bal.whole mech.vent no heat re": 0.7,
                "Bal.whole mech.vent heat recvr": 0.7,
                "Whole house extract vent.": 0.5,
                None: 0.5,
            }
        )
        .astype("float32"),
    )
    .merge(
        dublin_postcode_boundaries[["CountyName", "distance_to_city_centre_in_km"]],
        on="CountyName",
    )
)

dublin_ber_public.to_parquet(data_dir / "dublin_ber_public.parquet")
