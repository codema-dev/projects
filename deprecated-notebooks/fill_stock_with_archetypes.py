from pathlib import Path

import geopandas as gpd
import pandas as pd
import numpy as np

from dublin_building_stock.residential import create_latest_stock

DATA_DIR = Path("../data")


def fillna_with_archetypes(df, archetypes, archetype_columns):

    return df.set_index(archetype_columns).combine_first(archetypes).reset_index()


dublin_small_area_boundaries_2011 = gpd.read_file(
    DATA_DIR / "dublin_small_area_boundaries_2011.geojson", driver="GeoJSON"
)

census_2011_hh_indiv = (
    pd.read_parquet(DATA_DIR / "census_2011_hh_indiv.parquet")
    .assign(
        regulatory_period=lambda df: df["period_built"]
        .replace(
            {
                "before 1919": "before 1970",
                "1919 - 1945": "before 1970",
                "1946 - 1960": "before 1970",
                "1961 - 1970": "before 1970",
                "not stated": "before 1970",
            }
        )  # ~1/2 stock built prior to 1970
        .astype(
            pd.CategoricalDtype(
                categories=[
                    "before 1970",
                    "1971 - 1980",
                    "1981 - 1990",
                    "1991 - 2000",
                    "2001 - 2005",
                    "2006 - 2010",
                    "2011 or later",
                ],
                ordered=True,
            )
        ),
    )
    .drop(columns="EDNAME")
)
dublin_ber_private = pd.read_parquet(DATA_DIR / "dublin_ber_private.parquet")

archetypes_small_areas = (
    pd.read_parquet(DATA_DIR / "archetypes_small_area.parquet")
    .assign(archetype="small_area", archetype_partial="small_area")
    .rename_axis(index={"SMALL_AREA_2016": "SMALL_AREA"})
)

archetypes_electoral_districts = pd.read_parquet(
    DATA_DIR / "archetypes_electoral_district.parquet"
).assign(archetype="electoral_district", archetype_partial="electoral_district")

archetypes_all_of_dublin = pd.read_parquet(
    DATA_DIR / "archetypes_all_of_dublin.parquet"
).assign(archetype="all_of_dublin", archetype_partial="all_of_dublin")

archetypes_dwelling_type = pd.read_parquet(
    DATA_DIR / "archetypes_dwelling_type.parquet"
).assign(archetype="dwelling_type", archetype_partial="dwelling_type")

archetypes_period_built = pd.read_parquet(
    DATA_DIR / "archetypes_period_built.parquet"
).assign(archetype="period_built", archetype_partial="period_built")

use_columns = [
    "SMALL_AREA_2011",
    "BERBand",
    "Energy Value",
    "dwelling_type",
    "period_built",
    "Year of construction",
    "category_id",
    "total_floor_area",
    "Ventilation Method Description",
    "Wall weighted Uvalue",
    "Door Weighted Uvalue",
    "Roof Weighted Uvalue",
    "Floor Weighted Uvalue",
    "WindowsWeighted Uvalue",
    "Wall Total Area",
    "Door Total Area",
    "Roof Total Area",
    "Floor Total Area",
    "Windows Total Area",
    "effective_air_rate_change",
    "No Of Storeys",
    "Most Significant Wall Type",
]
dublin_ber_private_extract = (
    dublin_ber_private.loc[:, use_columns]
    .assign(
        archetype="none",
        regulatory_period=lambda df: pd.cut(
            df["period_built"].cat.codes,
            bins=[-np.inf, 3, 4, 5, 6, 7, 8, 9],
            labels=[
                "before 1970",
                "1971 - 1980",
                "1981 - 1990",
                "1991 - 2000",
                "2001 - 2005",
                "2006 - 2010",
                "2011 or later",
            ],
        ),
        most_significant_wall_type=lambda df: df["Most Significant Wall Type"].replace(
            {"Other": np.nan}
        ),
    )
    .rename(columns={"SMALL_AREA_2011": "SMALL_AREA"})
)

area_columns = [
    c
    for c in dublin_ber_private_extract.columns
    if "area" in c.lower()
    and not any(x in c.lower() for x in ["roof", "door", "small_area"])
]
# remove all zero floor, wall, window areas
dublin_ber_private_extract.loc[:, area_columns] = dublin_ber_private_extract[
    area_columns
].replace({0: np.nan})
dublin_ber_private_extract = dublin_ber_private_extract.assign(
    has_zero_areas=lambda df: df[area_columns].isnull().any(axis=1),
    archetype_partial=lambda df: df["has_zero_areas"].replace(
        {True: np.nan, False: "none"}
    ),
    sample_size=lambda df: df["archetype_partial"].where(
        df["archetype_partial"].isnull(), "n_a"
    ),
)

dublin_indiv_hh_before_2011 = census_2011_hh_indiv.merge(
    dublin_ber_private_extract,
    left_on=["SMALL_AREA", "dwelling_type", "period_built", "category_id"],
    right_on=["SMALL_AREA", "dwelling_type", "period_built", "category_id"],
    how="left",
    suffixes=["", "_BER"],
)

dublin_indiv_hh_2011_or_later = dublin_ber_private_extract.query(
    "`Year of construction` >= 2011"
)

dublin_indiv_hh_raw = pd.concat(
    [
        dublin_indiv_hh_before_2011,
        dublin_indiv_hh_2011_or_later,
    ]
)

drop_columns = [
    "category_id",
    "BERBand",
    "Year of construction",
    "Most Significant Wall Type",
] + [c for c in dublin_indiv_hh_before_2011 if "_ber" in c.lower()]
dublin_indiv_hh = (
    dublin_indiv_hh_raw.merge(
        dublin_small_area_boundaries_2011[["SMALL_AREA", "EDNAME", "local_authority"]],
        left_on="SMALL_AREA",
        right_on="SMALL_AREA",
        how="left",
    )
    .pipe(
        fillna_with_archetypes,
        archetypes=archetypes_small_areas,
        archetype_columns=["SMALL_AREA", "dwelling_type", "period_built"],
    )
    .pipe(
        fillna_with_archetypes,
        archetypes=archetypes_electoral_districts,
        archetype_columns=["EDNAME", "dwelling_type", "period_built"],
    )
    .pipe(
        fillna_with_archetypes,
        archetypes=archetypes_all_of_dublin,
        archetype_columns=["dwelling_type", "period_built"],
    )
    .pipe(
        fillna_with_archetypes,
        archetypes=archetypes_dwelling_type,
        archetype_columns=["dwelling_type"],
    )  # fills 'not stated' dwelling_type
    .pipe(
        fillna_with_archetypes,
        archetypes=archetypes_period_built,
        archetype_columns=["period_built"],
    )  # fills 'not stated' period_built
    .drop(columns=drop_columns)
    .query("local_authority.notnull()")
)

dublin_indiv_hh.assign(
    sample_size=lambda df: df["sample_size"].astype(str),
).to_parquet(DATA_DIR / "dublin_indiv_hh.parquet")
