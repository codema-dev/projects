from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

data_dir = Path("../data")

DATA_DIR = Path("../data")


def fillna_with_archetypes(df, archetypes, archetype_columns):

    return df.set_index(archetype_columns).combine_first(archetypes).reset_index()


dublin_small_area_boundaries_2011 = gpd.read_file(
    DATA_DIR / "dublin_small_area_boundaries_2011.geojson", driver="GeoJSON"
)

census_2011_dtypes = {
    "SMALL_AREA": "category",
    "EDNAME": "category",
    "dwelling_type": "category",
    "period_built": "category",
    "category_id": "int16",
}
census_2011_indiv_hhs = pd.read_parquet(
    data_dir / "census_2011_indiv_hhs.parquet"
).astype(census_2011_dtypes)

dublin_ber_private = pd.read_parquet(DATA_DIR / "dublin_ber_private.parquet")

ber_private_dtypes = {
    "SMALL_AREA_2011": "category",
    "dwelling_type": "category",
    "period_built": "category",
    "BERBand": "category",
    "Energy Value": "int16",
    "category_id": "int16",
    "Wall weighted Uvalue": "float32",
    "Door Weighted Uvalue": "float32",
    "Roof Weighted Uvalue": "float32",
    "Floor Weighted Uvalue": "float32",
    "WindowsWeighted Uvalue": "float32",
    "Wall Total Area": "float32",
    "Door Total Area": "float32",
    "Roof Total Area": "float32",
    "Floor Total Area": "float32",
    "Windows Total Area": "float32",
    "total_floor_area": "float32",
    "No Of Storeys": "int8",
    "HS Main System Efficiency": "float32",
    "Main SH Fuel Description": "category",
    "Ventilation Method Description": "category",
    "effective_air_rate_change": "float32",
    "most_significant_wall_type": "category",
}
column_names = list(ber_private_dtypes.keys())
dublin_ber_private_extract = (
    dublin_ber_private.loc[:, column_names]
    .assign(archetype="none")
    .astype(ber_private_dtypes)
    .rename(columns={"SMALL_AREA_2011": "SMALL_AREA"})
)

dublin_indiv_hhs_before_2011 = census_2011_indiv_hhs.merge(
    dublin_ber_private_extract,
    left_on=["SMALL_AREA", "dwelling_type", "period_built", "category_id"],
    right_on=["SMALL_AREA", "dwelling_type", "period_built", "category_id"],
    how="left",
    suffixes=["", "_BER"],
).combine_first(
    dublin_small_area_boundaries_2011[["SMALL_AREA", "distance_to_city_centre_in_km"]]
)

dublin_indiv_hhs_2011_or_later = (
    dublin_ber_private_extract.query("period_built == '2011 or later'")
    .rename(columns={"SMALL_AREA_2011": "SMALL_AREA"})
    .merge(
        dublin_small_area_boundaries_2011[
            ["SMALL_AREA", "distance_to_city_centre_in_km"]
        ]
    )
)

dublin_indiv_hhs_dtypes = {
    "SMALL_AREA": "category",
    "EDNAME": "category",
    "period_built": "category",
}
dublin_indiv_hhs = pd.concat(
    [
        dublin_indiv_hhs_before_2011,
        dublin_indiv_hhs_2011_or_later,
    ]
).astype(dublin_indiv_hhs_dtypes)

dublin_indiv_hhs_known = dublin_indiv_hhs.query("archetype == 'none'")
dublin_indiv_hhs_unknown = dublin_indiv_hhs.query("archetype != 'none'")

dublin_indiv_hhs_known.to_parquet(data_dir / "dublin_indiv_hhs_known.parquet")
dublin_indiv_hhs_unknown.to_parquet(data_dir / "dublin_indiv_hhs_unknown.parquet")
