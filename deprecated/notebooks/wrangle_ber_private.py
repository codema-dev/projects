from pathlib import Path

import numpy as np
import pandas as pd

data_dir = Path("../data")

small_areas_2011_vs_2016 = pd.read_csv(data_dir / "small_areas_2011_vs_2016.csv")

heat_loss_parameter_cutoff = 2.3  # SEAI, Tech Advisor Role Heat Pumps 2020
thermal_bridging_factor = 0.15  # 87% of ThermalBridgingFactor in BER Public
# 90% of GroundFloorHeight within (2.25, 2.75) in BER Publiczz
assumed_floor_height = 2.5

dublin_ber_private = (
    pd.read_csv(data_dir / "BER.09.06.2020.csv")
    .query("CountyName2.str.contains('DUBLIN')")
    .merge(
        small_areas_2011_vs_2016,
        left_on="cso_small_area",
        right_on="SMALL_AREA_2016",
    )  # Link 2016 SAs to 2011 SAs as best census data is 2011
    .assign(
        SMALL_AREA_2011=lambda df: df["SMALL_AREA_2011"].astype(str),
        BERBand=lambda df: df["Energy Rating"].str[0],
        period_built=lambda df: pd.cut(
            df["Year of construction"],
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
        dwelling_type=lambda df: df["Dwelling type description"].replace(
            {
                "Grnd floor apt.": "Apartment",
                "Mid floor apt.": "Apartment",
                "Top floor apt.": "Apartment",
                "Maisonette": "Apartment",
                "Apt.": "Apartment",
                "Det. house": "Detached house",
                "Semi-det. house": "Semi-detached house",
                "House": "Semi-detached house",
                "Mid terrc house": "Terraced house",
                "End terrc house": "Terraced house",
                None: "Not stated",
            }
        ),
        total_floor_area=lambda df: df["Ground Floor Area"]
        + df["First Floor Area"]
        + df["Second Floor Area"]
        + df["Third Floor Area"],
        effective_air_rate_change=lambda df: df["Ventilation Method Description"]
        .replace(
            {
                "Natural vent.": 0.52,
                "Bal.whole mech.vent heat recvry": 0.7,
                "Pos input vent.- loft": 0.51,
                "Bal.whole mech.vent no heat recvry": 0.7,
                "Whole house extract vent.": 0.5,
                "Pos input vent.- outside": 0.5,
                None: 0.5,
            }
        )
        .astype("float32"),
        most_significant_wall_type=lambda df: df["Most Significant Wall Type"].replace(
            {"Other": np.nan}
        ),
        category_id=lambda df: df.groupby(
            ["SMALL_AREA_2011", "dwelling_type", "period_built"]
        )
        .cumcount()
        .apply(lambda x: x + 1),
    )
    .drop(columns=["cso_small_area", "geo_small_area"])
)

dublin_ber_private.to_parquet(data_dir / "dublin_ber_private.parquet")
