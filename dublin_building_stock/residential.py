import dask.dataframe as dd
import geopandas as gpd
import numpy as np
import pandas as pd


from dublin_building_stock.spatial_operations import get_geometries_within
from dublin_building_stock.deap import calculate_heat_loss_parameter


def _repeat_rows_on_column(df, on):
    return df.reindex(df.index.repeat(df[on])).drop(columns=on)


def _extract_census_columns(filepath, boundary, column_names, category, melt=True):
    use_columns = ["SMALL_AREA"] + list(column_names.values())

    if melt:
        extracted_columns = (
            pd.read_csv(filepath)
            .rename(columns=column_names)
            .assign(SMALL_AREA=lambda df: df["GEOGID"].str[7:])
            .loc[:, use_columns]
            .merge(boundary[["SMALL_AREA", "EDNAME"]])
            .melt(id_vars=["EDNAME", "SMALL_AREA"], var_name=category)
            .assign(value=lambda df: df["value"].astype(np.int32))
        )
    else:
        extracted_columns = (
            pd.read_csv(filepath)
            .rename(columns=column_names)
            .assign(SMALL_AREA=lambda df: df["GEOGID"].str[7:])
            .loc[:, use_columns]
            .merge(boundary["SMALL_AREA"])
        )

    return extracted_columns


def create_census_2016_hh_age(data_dir, dublin_small_area_boundaries_2016):
    column_names = {
        "T6_2_PRE19H": "before 1919",
        "T6_2_19_45H": "1919 - 1945",
        "T6_2_46_60H": "1946 - 1960",
        "T6_2_61_70H": "1961 - 1970",
        "T6_2_71_80H": "1971 - 1980",
        "T6_2_81_90H": "1981 - 1990",
        "T6_2_91_00H": "1991 - 2000",
        "T6_2_01_10H": "2001 - 2010",
        "T6_2_11LH": "2011 or later",
        "T6_2_NSH": "not stated",
        "T6_2_TH": "total",
    }
    census_2016_hh_age = _extract_census_columns(
        data_dir / "SAPS2016_SA2017.csv",
        dublin_small_area_boundaries_2016,
        column_names,
        "period_built",
    )
    census_2016_hh_age.to_csv(data_dir / "census_2016_hh_age.csv", index=False)


def create_census_2016_hh_type(data_dir, dublin_small_area_boundaries_2016):
    column_names = {
        "T6_1_HB_H": "House/Bungalow",
        "T6_1_FA_H": "Flat/Apartment",
        "T6_1_BS_H": "Bed-Sit",
        "T6_1_CM_H": "Caravan/Mobile home",
        "T6_1_CM_H": "Not Stated",
        "T6_1_TH": "Total",
    }
    census_2016_hh_type = _extract_census_columns(
        data_dir / "SAPS2016_SA2017.csv",
        dublin_small_area_boundaries_2016,
        column_names,
        "hh_type",
    )
    census_2016_hh_type.to_csv(data_dir / "census_2016_hh_type.csv", index=False)


def create_census_2016_hh_boilers(data_dir, dublin_small_area_boundaries_2016):
    column_names = {
        "T6_5_NCH": "No central heating",
        "T6_5_OCH": "Oil",
        "T6_5_NGCH": "Natural gas",
        "T6_5_ECH": "Electricity",
        "T6_5_CCH": "Coal (incl. anthracite)",
        "T6_5_PCH": "Peat (incl. turf)",
        "T6_5_LPGCH": "Liquid petroleum gas (LPG)",
        "T6_5_WCH": "Wood (incl. wood pellets)",
        "T6_5_OTH": "Other",
        "T6_5_NS": "Not stated",
        "T6_5_T": "Total",
    }
    census_2016_hh_boilers = _extract_census_columns(
        data_dir / "SAPS2016_SA2017.csv",
        dublin_small_area_boundaries_2016,
        column_names,
        "boiler_type",
        melt=False,
    )
    census_2016_hh_boilers.to_csv(data_dir / "census_2016_hh_boilers.csv", index=False)


def create_census_2016_hh_age_indiv(data_dir, census_2016_hh_age):
    census_2016_hh_age_indiv = (
        _repeat_rows_on_column(census_2016_hh_age, "value")
        .query("period_built != ['total']")
        .assign(
            estimated_ber=lambda df: df["period_built"].replace(
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
                    "not stated": "unknown",
                }
            ),
            ber_kwh_per_m2_y=lambda df: df["estimated_ber"]
            .replace(
                {
                    "A": 25,
                    "B": 100,
                    "C": 175,
                    "D": 240,
                    "E": 320,
                    "F": 380,
                    "G": 450,
                    "unknown": 240,
                }
            )
            .astype(np.int32),
        )
    )
    census_2016_hh_age_indiv.to_csv(
        data_dir / "census_2016_hh_age_indiv.csv", index=False
    )


def create_census_2011_small_area_hhs(data_dir, dublin_small_area_boundaries_2011):
    filepaths = [
        pd.read_csv(
            data_dir / "Census-2011-crosstabulations" / f"{local_authority}_SA_2011.csv"
        )
        for local_authority in ["DCC", "DLR", "FCC", "SD"]
    ]
    census_2011_small_area_hhs = (
        pd.concat(filepaths)
        .query("`sa_2011` != ['Dublin City', 'South Dublin']")
        .query("`period_built_unstandardised` != ['Total', 'All Houses']")
        .replace({">3": 1, "<3": 1, ".": np.nan})
        .dropna(subset=["value"])
        .rename(
            columns={
                "sa_2011": "SMALL_AREA",
                "period_built_unstandardised": "period_built",
            }
        )
        .assign(
            value=lambda df: df["value"].astype(np.int32),
            SMALL_AREA=lambda df: df["SMALL_AREA"].str.replace(r"_", r"/"),
            period_built=lambda df: df["period_built"]
            .str.lower()
            .str.replace("2006 or later", "2006 - 2010"),
            dwelling_type=lambda df: df["dwelling_type_unstandardised"].replace(
                {
                    "Flat/apartment in a purpose-built block": "Apartment",
                    "Flat/apartment in a converted house or commercial building": "Apartment",
                    "Bed-sit": "Apartment",
                }
            ),
        )
        .merge(
            dublin_small_area_boundaries_2011[["SMALL_AREA", "EDNAME"]],
            indicator=True,
            how="outer",
        )
        .reset_index(drop=True)
        .drop(columns=["dwelling_type_unstandardised"])
    )
    census_2011_small_area_hhs.drop(columns="_merge").to_parquet(
        data_dir / "census_2011_small_area_hhs.parquet",
    )


def create_census_2011_hh_indiv(data_dir, census_2011_small_area_hhs):
    census_2011_hh_indiv = (
        _repeat_rows_on_column(census_2011_small_area_hhs, "value")
        .query("period_built != ['total']")
        .assign(
            category_id=lambda df: df.groupby(
                ["SMALL_AREA", "dwelling_type", "period_built"]
            )
            .cumcount()
            .apply(lambda x: x + 1),
            EDNAME=lambda df: df["EDNAME"]
            .str.normalize("NFKD")
            .str.encode("ascii", errors="ignore")
            .str.decode("utf-8")
            .str.replace(r"[-]", " ", regex=True)
            .str.replace(r"[,'.]", "", regex=True)
            .str.capitalize(),
        )
    )
    census_2011_hh_indiv.to_parquet(data_dir / "census_2011_hh_indiv.parquet")


def create_dublin_ber_public(filepath):
    ber_public = dd.read_parquet(filepath)
    # 90% of GroundFloorHeight within (2.25, 2.75) in BER Public
    assumed_floor_height = 2.5
    heat_loss_parameter_cutoff = 2.3  # SEAI, Tech Advisor Role Heat Pumps 2020
    return (
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
            heat_loss_parameter=lambda df: calculate_heat_loss_parameter(
                roof_area=df["RoofArea"],
                roof_uvalue=df["UValueRoof"],
                wall_area=df["WallArea"],
                wall_uvalue=df["UValueWall"],
                floor_area=df["FloorArea"],
                floor_uvalue=df["UValueFloor"],
                window_area=df["WindowArea"],
                window_uvalue=df["UValueWindow"],
                door_area=df["DoorArea"],
                door_uvalue=df["UvalueDoor"],
                total_floor_area=df["total_floor_area"],
                thermal_bridging_factor=df["ThermalBridgingFactor"],
                effective_air_rate_change=df["effective_air_rate_change"],
                ground_floor_area=df["GroundFloorArea"],
                ground_floor_height=df["GroundFloorHeight"],
                first_floor_area=df["FirstFloorArea"],
                first_floor_height=df["FirstFloorHeight"],
                second_floor_area=df["SecondFloorArea"],
                second_floor_height=df["SecondFloorHeight"],
                third_floor_area=df["ThirdFloorArea"],
                third_floor_height=df["ThirdFloorHeight"],
                assumed_floor_height=assumed_floor_height,
            ),
            heat_pump_ready=lambda df: pd.cut(
                df["heat_loss_parameter"],
                bins=[0, heat_loss_parameter_cutoff, np.inf],
                labels=[True, False],
            ).astype("bool"),
        )
    )


def create_dublin_ber_private(data_dir, small_areas_2011_vs_2016):
    heat_loss_parameter_cutoff = 2.3  # SEAI, Tech Advisor Role Heat Pumps 2020
    thermal_bridging_factor = 0.15  # 87% of ThermalBridgingFactor in BER Public
    # 90% of GroundFloorHeight within (2.25, 2.75) in BER Publiczz
    assumed_floor_height = 2.5
    return (
        pd.read_csv(data_dir / "BER.09.06.2020.csv")
        .query("CountyName2.str.contains('DUBLIN')")
        .merge(
            small_areas_2011_vs_2016,
            left_on="cso_small_area",
            right_on="SMALL_AREA_2016",
        )  # Link 2016 SAs to 2011 SAs as best census data is 2011
        .assign(
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
            category_id=lambda df: df.groupby(
                ["SMALL_AREA_2011", "dwelling_type", "period_built"]
            )
            .cumcount()
            .apply(lambda x: x + 1),
            total_floor_area=lambda df: df["Ground Floor Area"]
            + df["First Floor Area"]
            + df["Second Floor Area"]
            + df["Third Floor Area"],
            SMALL_AREA_2011=lambda df: df["SMALL_AREA_2011"].astype(str),
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
            heat_loss_parameter=lambda df: calculate_heat_loss_parameter(
                roof_area=df["Roof Total Area"],
                roof_uvalue=df["Roof Weighted Uvalue"],
                wall_area=df["Wall Total Area"],
                wall_uvalue=df["Wall weighted Uvalue"],
                floor_area=df["Floor Total Area"],
                floor_uvalue=df["Floor Weighted Uvalue"],
                window_area=df["Windows Total Area"],
                window_uvalue=df["WindowsWeighted Uvalue"],
                door_area=df["Door Total Area"],
                door_uvalue=df["Door Weighted Uvalue"],
                total_floor_area=df["total_floor_area"],
                thermal_bridging_factor=thermal_bridging_factor,
                effective_air_rate_change=df["effective_air_rate_change"],
                no_of_storeys=df["No Of Storeys"],
                assumed_floor_height=assumed_floor_height,
            ),
            heat_pump_ready=lambda df: pd.cut(
                df["heat_loss_parameter"],
                bins=[0, heat_loss_parameter_cutoff, np.inf],
                labels=[True, False],
            ).astype("bool"),
        )
        .drop(columns=["cso_small_area", "geo_small_area"])
    )


def _infer_heat_pump_readiness(df):
    return df.assign(
        heat_pump_readiness_estimated_on_period_built=lambda df: df[
            "period_built"
        ].replace(
            {
                "before 1919": False,
                "1919 - 1945": False,
                "1946 - 1960": False,
                "1961 - 1970": False,
                "1971 - 1980": False,
                "1981 - 1990": False,
                "1991 - 2000": True,
                "2001 - 2005": True,
                "2006 - 2010": True,
                "2011 or later": True,
                "not stated": False,
            }
        ),
        inferred_heat_pump_readiness=lambda df: df["heat_pump_ready"]
        .fillna(df["heat_pump_readiness_estimated_on_period_built"])
        .astype("bool"),
    )


def _infer_dimensions(df):
    return df.assign(
        dwelling_type_floor_area=lambda df: df.groupby("dwelling_type")[
            "total_floor_area"
        ]
        .apply(lambda x: x.fillna(x.median()))
        .round(),
        inferred_floor_area=lambda df: df["total_floor_area"].fillna(
            df["dwelling_type_floor_area"]
        ),
        dwelling_type_roof_area=lambda df: df.groupby("dwelling_type")[
            "Roof Total Area"
        ]
        .apply(lambda x: x.fillna(x.median()))
        .round(),
        inferred_roof_area=lambda df: df["Roof Total Area"].fillna(
            df["dwelling_type_roof_area"]
        ),
        dwelling_type_door_area=lambda df: df.groupby("dwelling_type")[
            "Door Total Area"
        ]
        .apply(lambda x: x.fillna(x.median()))
        .round(),
        inferred_door_area=lambda df: df["Door Total Area"].fillna(
            df["dwelling_type_door_area"]
        ),
        dwelling_type_wall_area=lambda df: df.groupby("dwelling_type")[
            "Wall Total Area"
        ]
        .apply(lambda x: x.fillna(x.median()))
        .round(),
        inferred_wall_area=lambda df: df["Wall Total Area"].fillna(
            df["dwelling_type_wall_area"]
        ),
        dwelling_type_window_area=lambda df: df.groupby("dwelling_type")[
            "Windows Total Area"
        ]
        .apply(lambda x: x.fillna(x.median()))
        .round(),
        inferred_window_area=lambda df: df["Windows Total Area"].fillna(
            df["dwelling_type_window_area"]
        ),
        dwelling_type_no_storeys=lambda df: df.groupby("dwelling_type")["No Of Storeys"]
        .apply(lambda x: x.fillna(x.median()))
        .round(),
        inferred_no_storeys=lambda df: df["No Of Storeys"].fillna(
            df["dwelling_type_no_storeys"]
        ),
    )


def _infer_ber_rating(df):
    return df.assign(
        ber_estimated_on_period_built=lambda df: df["period_built"].replace(
            {
                "before 1919": "E",
                "1919 - 1945": "E",
                "1946 - 1960": "E",
                "1961 - 1970": "D",
                "1971 - 1980": "D",  # uncertain
                "1981 - 1990": "D",  # uncertain
                "1991 - 2000": "D",
                "2001 - 2005": "C",
                "2006 - 2010": "B",
                "2011 or later": "A",
                "not stated": "unknown",
            }
        ),
        inferred_ber=lambda df: np.where(
            df["BERBand"].isnull(),
            df["ber_estimated_on_period_built"],
            df["BERBand"],
        ),
    )


def _infer_uvalues(df):
    return df.assign(
        uvalue_wall_estimated_on_period_built=lambda df: df["period_built"].replace(
            {
                "before 1919": 1.5,  # 1.25-1.75
                "1919 - 1945": 1.5,  # 0-0.5 & 1.25-2
                "1946 - 1960": 1.5,  # 0-0.5 & 1.25-2
                "1961 - 1970": 1.5,  # 0-0.5 & 1.25-2.5
                "1971 - 1980": 1,  # 0-0.5 & 1-1.25 & 1.75-2
                "1981 - 1990": 0.6,  # 0.5-0.75
                "1991 - 2000": 0.6,  # 0.5-0.75
                "2001 - 2005": 0.6,  # 0.5-0.75
                "2006 - 2010": 0.4,  # 0-0.5
                "2011 or later": 0.2,  # 0-0.25
                "not stated": 1,  # estimated
            }
        ),
        inferred_uvalue_wall=lambda df: df["Wall weighted Uvalue"].fillna(
            df["uvalue_wall_estimated_on_period_built"]
        ),
        uvalue_roof_estimated_on_period_built=lambda df: df["period_built"].replace(
            {
                "before 1919": 0.5,  # 0-1
                "1919 - 1945": 0.5,  # 0-1
                "1946 - 1960": 0.5,  # 0-1
                "1961 - 1970": 0.5,  # 0-1
                "1971 - 1980": 0.5,  # 0-1
                "1981 - 1990": 0.3,  # 0-0.5
                "1991 - 2000": 0.3,  # 0-0.5
                "2001 - 2005": 0.3,  # 0-0.5
                "2006 - 2010": 0.15,  # 0-0.25
                "2011 or later": 0.15,  # 0-0.25
                "not stated": 0.5,  # estimated
            }
        ),
        inferred_uvalue_roof=lambda df: df["Roof Weighted Uvalue"].fillna(
            df["uvalue_roof_estimated_on_period_built"]
        ),
        uvalue_window_estimated_on_period_built=lambda df: df["period_built"].replace(
            {
                "before 1919": 3,  # 2.75-3.5
                "1919 - 1945": 3,  # 2.5-3.25
                "1946 - 1960": 3,  # 2.75-3.25
                "1961 - 1970": 3,  # 2.75-3.25
                "1971 - 1980": 3,  # 2.75-3.25
                "1981 - 1990": 3,  # 2.75-3.25
                "1991 - 2000": 3,  # 2.75-3.25
                "2001 - 2005": 2.7,  # 2-3
                "2006 - 2010": 2.2,  # 1-3
                "2011 or later": 1.3,  # 1-1.5
                "not stated": 3,  # estimated
            }
        ),
        inferred_uvalue_window=lambda df: df["WindowsWeighted Uvalue"].fillna(
            df["uvalue_window_estimated_on_period_built"]
        ),
        uvalue_floor_estimated_on_period_built=lambda df: df["period_built"].replace(
            {
                "before 1919": 0.6,  # 0.5-1
                "1919 - 1945": 0.6,  # 0.5-1
                "1946 - 1960": 0.6,  # 0.5-1
                "1961 - 1970": 0.6,  # 0.5-1
                "1971 - 1980": 0.6,  # 0.5-1
                "1981 - 1990": 0.6,  # 0.5-1
                "1991 - 2000": 0.4,  # 0-0.6
                "2001 - 2005": 0.3,  # 0-0.4
                "2006 - 2010": 0.2,  # 0-0.5
                "2011 or later": 0.1,  # 0-0.2
                "not stated": 0.6,  # estimated
            }
        ),  # all < 1990 strong mode at 0.6
        inferred_uvalue_floor=lambda df: df["Floor Weighted Uvalue"].fillna(
            df["uvalue_floor_estimated_on_period_built"]
        ),
        uvalue_door_estimated_on_period_built=lambda df: df["period_built"].replace(
            {
                "before 1919": 0.6,  # 0.5-1
                "1919 - 1945": 3,  # 0 & 3
                "1946 - 1960": 3,  # 0 & 3
                "1961 - 1970": 3,  # 0 & 3
                "1971 - 1980": 3,  # 0 & 3
                "1981 - 1990": 3,  # 0 & 3
                "1991 - 2000": 3,  # 0 & 1.5 & 3
                "2001 - 2005": 3,  # 0 & 1.5 & 3
                "2006 - 2010": 3,  # 0 & 1.5 & 3
                "2011 or later": 1.5,  # 0 & 1.5 & 3
                "not stated": 3,  # estimated
            }
        ),  # all strong mode at 3 except >2011
        inferred_uvalue_door=lambda df: df["Door Weighted Uvalue"].fillna(
            df["uvalue_door_estimated_on_period_built"]
        ),
    )


def _infer_boiler_efficiencies(df):
    return df.assign(
        inferred_boiler_efficiency=lambda df: df["HS Main System Efficiency"].fillna(
            df[["HS Main System Efficiency"]]
            .query("`HS Main System Efficiency` < 100")
            .squeeze()
            .round()
            .median()
        ),
    )


def _estimate_energy_kwh_per_m2_year(df):

    return df.assign(
        energy_estimated_on_ber_band=lambda df: df["inferred_ber"]
        .replace(
            {
                "A": 25,  # -inf-0.75
                "B": 100,  # 75-150
                "C": 180,  # 150-225
                "D": 260,  # 225-300
                "E": 330,  # 300-360
                "F": 400,  # 360-460
                "G": 500,  # 450-inf
                "unknown": 260,
            }
        )
        .astype(np.int32),
        inferred_energy_kwh_per_m2_year=lambda df: df["Energy Value"].fillna(
            df["energy_estimated_on_ber_band"]
        ),
    )


def _estimate_annual_heat_demand(df):
    deap_energy_used_for_heating = 0.8  # Energy in the Residential Sector, SEAI 2018
    typical_boiler_efficiency = 0.85
    kwh_to_mwh_conversion_factor = 10 ** -3
    return df.assign(
        heating_mwh_per_m2_year=lambda df: df["inferred_energy_kwh_per_m2_year"]
        * typical_boiler_efficiency
        * deap_energy_used_for_heating
        * kwh_to_mwh_conversion_factor,
        heating_mwh_per_year=lambda df: df["heating_mwh_per_m2_year"]
        * df["inferred_floor_area"],
    )


def create_latest_stock(
    data_dir,
    census_2011_hh_indiv,
    dublin_ber_private,
):
    keep_columns = [
        "SMALL_AREA",
        "SMALL_AREA_2011",
        "EDNAME",
        "dwelling_type",
        "period_built",
        "total_floor_area",
        "Wall Total Area",
        "Roof Total Area",
        "Windows Total Area",
        "Door Total Area",
        "No Of Storeys",
        "BERBand",
        "Floor Weighted Uvalue",
        "Wall weighted Uvalue",
        "Roof Weighted Uvalue",
        "WindowsWeighted Uvalue",
        "Door Weighted Uvalue",
        "HS Main System Efficiency",
        "heat_pump_ready",
        "Energy Value",
    ]
    dublin_indiv_hh_before_2011 = census_2011_hh_indiv.merge(
        dublin_ber_private,
        left_on=["SMALL_AREA", "dwelling_type", "period_built", "category_id"],
        right_on=["SMALL_AREA_2011", "dwelling_type", "period_built", "category_id"],
        how="left",
        indicator=True,
        suffixes=["", "_BER"],
    )

    dublin_indiv_hh_2011_or_later = dublin_ber_private.query(
        "`Year of construction` >= 2011"
    )

    keep_columns = [
        "is_from_ber_sample",
        "EDNAME",
        "SMALL_AREA",
        "dwelling_type",
        "period_built",
        "inferred_ber",
        "inferred_energy_kwh_per_m2_year",
        "inferred_floor_area",
        "inferred_wall_area",
        "inferred_roof_area",
        "inferred_window_area",
        "inferred_door_area",
        "inferred_no_storeys",
        "inferred_uvalue_floor",
        "inferred_uvalue_wall",
        "inferred_uvalue_roof",
        "inferred_uvalue_window",
        "inferred_uvalue_door",
        "inferred_boiler_efficiency",
    ]
    dublin_indiv_hh = (
        pd.concat(
            [
                dublin_indiv_hh_before_2011,
                dublin_indiv_hh_2011_or_later,
            ]
        )
        .reset_index(drop=True)
        .pipe(_infer_dimensions)
        .pipe(_infer_ber_rating)
        .pipe(_infer_uvalues)
        .pipe(_infer_boiler_efficiencies)
        .pipe(_estimate_energy_kwh_per_m2_year)
        .assign(
            SMALL_AREA=lambda df: df["SMALL_AREA"].fillna(
                df["SMALL_AREA_2011"].astype(str)
            ),
            EDNAME=lambda df: df["EDNAME"].fillna(df["EDNAME_BER"]),
            is_from_ber_sample=lambda df: df["CountyName2"].notnull(),
        )
        .loc[:, keep_columns]
    )
    dublin_indiv_hh.to_parquet(data_dir / "dublin_indiv_hh.parquet")


def anonymise_census_2011_hh_indiv_to_routing_key_boundaries(
    data_dir,
    dublin_indiv_hh_2011,
    dublin_routing_key_boundaries,
    dublin_small_area_boundaries_2011,
):
    small_areas_linked_to_postcodes = get_geometries_within(
        dublin_small_area_boundaries_2011,
        dublin_routing_key_boundaries,
    ).drop(columns="EDNAME")
    dublin_indiv_hh_2011_anonymised = dublin_indiv_hh_2011.merge(
        small_areas_linked_to_postcodes, on="SMALL_AREA"
    ).drop(columns=["category_id", "SMALL_AREA", "EDNAME", "geometry"])
    dublin_indiv_hh_2011_anonymised.to_csv(
        data_dir / "dublin_indiv_hh_2011_anonymised.csv", index=False
    )
