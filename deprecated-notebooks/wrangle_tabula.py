from pathlib import Path

import camelot
import numpy as np
import pandas as pd

data_dir = Path("../data")


def _extract_boiler_type(s: pd.Series) -> pd.Series:
    return s.astype(str).str.extract(r"IE\.([A-Za-z_]+)\..*")[0]


def _extract_efficiency_code(s: pd.Series) -> pd.Series:
    return s.astype(str).str.extract(r"\w+, ?(\w+\.? ?\w*?) efficiency.*").fillna("n_a")


tabula_codes = camelot.read_pdf(
    str(data_dir / "IE_TABULA_TypologyBrochure_EnergyAction.pdf"),
    pages="13",
)[0].df
tabula_codes.columns = (
    tabula_codes.iloc[0]
    .str.replace(f"(\n| )", "_", regex=True)
    .str.replace(r"(\.|:)", "", regex=True)
    .str.lower()
)
tabula_codes = tabula_codes.drop(0)

use_columns = [
    "Code_BuildingVariant",
    "ID",
    "Variant",
    "U_Roof_1",
    "U_Roof_2",
    "U_Wall_1",
    "U_Wall_2",
    "U_Wall_3",
    "U_Floor_1",
    "U_Floor_2",
    "U_Window_1",
    "U_Window_2",
    "U_Door_1",
    "U_Measure_Wall_1",
    "U_Measure_Wall_2",
    "U_Measure_Wall_3",
    "U_Measure_Roof_1",
    "U_Measure_Roof_2",
    "U_Measure_Wall_1",
    "U_Measure_Wall_2",
    "U_Measure_Wall_3",
    "U_Measure_Floor_1",
    "U_Measure_Floor_2",
    "U_Measure_Window_1",
    "U_Measure_Window_2",
    "U_Measure_Door_1",
    "Code_Measure_Wall_1",
    "Code_Measure_Wall_2",
    "Code_Measure_Wall_3",
    "Code_Measure_Roof_1",
    "Code_Measure_Roof_2",
    "Code_Measure_Wall_1",
    "Code_Measure_Wall_2",
    "Code_Measure_Wall_3",
    "Code_Measure_Floor_1",
    "Code_Measure_Floor_2",
    "Code_Measure_Window_1",
    "Code_Measure_Window_2",
    "Code_Measure_Door_1",
]
tabula_building_fabric = (
    pd.read_excel(
        data_dir / "tabula-calculator.xlsx",
        skiprows=[1, 2, 3, 4, 5],
        sheet_name="Calc.Set.Building",
    )
    .query(
        "Code_Country == 'IE' & Code_DataType_Building == 'ReEx'"
    )  # Extract Irish 'Real Examples' (ReEx)
    .assign(
        ID=lambda df: df["Code_BuildingVariant"].str.extract(
            r"IE\.N\.(\w+\.\d+.\w+).*"
        )[0],
        Variant=lambda df: df["Number_BuildingVariant"].map(
            {1: "existing", 2: "standard", 3: "advanced"}
        ),
        dwelling_type=lambda df: df["Code_BuildingSizeClass"].map(
            {
                "SFH": "Single Family House",
                "TH": "Terraced House",
                "AB": "Apartment",
                "MFH": "Multi Family House",
            }
        ),
    )
    .loc[:, use_columns]
)
tabula_building_fabric_existing = tabula_building_fabric.query(
    "Variant == 'existing'"
).drop(columns="Variant")
tabula_building_fabric_standard = tabula_building_fabric.query(
    "Variant == 'standard'"
).drop(columns="Variant")
tabula_building_fabric_advanced = tabula_building_fabric.query(
    "Variant == 'advanced'"
).drop(columns="Variant")


use_columns = [
    "Code_BuiSysCombi",
    "Code_BuildingVariant",
    "Code_System_AssignedMeasure",
    "Description_System_AssignedMeasure",
    "Code_BoundaryCond",
    "SysW_G_1",
    "SysW_G_2",
    "SysW_G_3",
    "Code_SysW_EC_1",
    "Code_SysW_EC_2",
    "Code_SysW_EC_3",
    "SysH_G_1",
    "SysH_G_2",
    "SysH_G_3",
    "Code_SysH_EC_1",
    "Code_SysH_EC_2",
    "Code_SysH_EC_3",
]
tabula_building_system_raw = (
    pd.read_excel(
        data_dir / "tabula-calculator.xlsx",
        skiprows=[1, 2, 3, 4, 5],
        sheet_name="Calc.Set.System",
    )
    .query("Code_Country == 'IE'")
    .assign(
        SysW_G_1=lambda df: df["Code_SysW_G_1"].pipe(_extract_boiler_type),
        SysW_G_2=lambda df: df["Code_SysW_G_2"].pipe(_extract_boiler_type),
        SysW_G_3=lambda df: df["Code_SysW_G_3"].pipe(_extract_boiler_type),
        SysH_G_1=lambda df: df["Code_SysH_G_1"].pipe(_extract_boiler_type),
        SysH_G_2=lambda df: df["Code_SysH_G_2"].pipe(_extract_boiler_type),
        SysH_G_3=lambda df: df["Code_SysH_G_3"].pipe(_extract_boiler_type),
    )
)

building_measures = (
    tabula_building_system_raw["Description_System_AssignedMeasure"]
    .str.split("/ ", expand=True)
    .rename(columns={0: "existing_system", 1: "standard_system", 2: "advanced_system"})
)

use_columns = [
    "Code_BuiSysCombi",
    "Code_BuildingVariant",
    "ID",
    "Variant",
    "boiler_efficiency",
    "SysW_G_1",
    "SysW_G_2",
    "SysW_G_3",
    "Code_SysW_EC_1",
    "Code_SysW_EC_2",
    "Code_SysW_EC_3",
    "SysH_G_1",
    "SysH_G_2",
    "SysH_G_3",
    "Code_SysH_EC_1",
    "Code_SysH_EC_2",
    "Code_SysH_EC_3",
] + building_measures.columns.tolist()
tabula_building_system = (
    tabula_building_system_raw.assign(
        Variant=lambda df: df["Code_BuildingVariant"]
        .str[-2:]
        .map({"01": "existing", "02": "standard", "03": "advanced"}),
    )
    .join(building_measures)
    .assign(
        ID=lambda df: df["Code_BuildingVariant"].str.extract(
            r"IE\.N\.(\w+\.\d+.\w+).*"
        )[0],
        boiler_efficiency_code=lambda df: df["existing_system"].pipe(
            _extract_efficiency_code
        ),
        boiler_efficiency=lambda df: df["boiler_efficiency_code"].map(
            {
                "poor": 0.65,
                "improved": 0.7,  # assumption
                "medium": 0.8,  # assumption
                "v. Good": 0.9,
                "n_a": 1,  # electric boilersS
            }
        ),
    )
    .loc[:, use_columns]
)
tabula_building_system_existing = tabula_building_system.query(
    "Variant == 'existing'"
).drop(columns="Variant")
tabula_building_system_standard = tabula_building_system.query(
    "Variant == 'standard'"
).drop(columns="Variant")
tabula_building_system_advanced = tabula_building_system.query(
    "Variant == 'advanced'"
).drop(columns="Variant")

tabula_typologies_existing = (
    tabula_building_fabric_existing.merge(
        tabula_building_system_existing,
        on=["Code_BuildingVariant", "ID"],
    )
    .merge(
        tabula_codes,
        left_on="ID",
        right_on="house_type",
    )
    .drop(columns=["standard__measures", "advanced__measures"])
).replace({0: np.nan})
tabula_typologies_to_standard = (
    tabula_building_fabric_standard.merge(
        tabula_building_system_standard,
        on=["Code_BuildingVariant", "ID"],
    )
    .merge(
        tabula_codes,
        left_on="ID",
        right_on="house_type",
    )
    .drop(columns=["advanced__measures"])
).replace({0: np.nan})
tabula_typologies_to_advanced = (
    tabula_building_fabric_advanced.merge(
        tabula_building_system_advanced,
        on=["Code_BuildingVariant", "ID"],
    )
    .merge(
        tabula_codes,
        left_on="ID",
        right_on="house_type",
    )
    .drop(columns=["standard__measures"])
).replace({0: np.nan})

tabula_typologies_to_standard.to_csv(
    data_dir / "tabula_typologies_to_standard.csv", index=False
)
tabula_typologies_to_advanced.to_csv(
    data_dir / "tabula_typologies_to_advanced.csv", index=False
)
