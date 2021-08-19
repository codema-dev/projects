from dotenv import load_dotenv

load_dotenv(".prefect")  # load local prefect configuration prior to import!
from prefect import Flow

import tasks
from globals import HERE
from globals import DATA_DIR

INPUT_FILEPATHS = {
    "public_sector_buildings": DATA_DIR
    / "external"
    / "monitoring_and_reporting_geocoded_by_osm.csv",
    "residential_buildings": DATA_DIR / "external" / "small_area_bers.parquet",
    "commercial_buildings": DATA_DIR / "external" / "valuation_office_demands.csv",
}

OUTPUT_FILEPATHS = {
    "plot": {
        "tco2": HERE / "tco2.png",
        "mwh": HERE / "mwh.png",
    },
    "data": {
        "tco2": DATA_DIR / "processed" / "tco2.csv",
        "mwh": DATA_DIR / "processed" / "mwh.csv",
    },
}

FUELS_TO_EMISSION_FACTORS_KWH_TO_TCO2 = {
    "Mains Gas": 204.7e-6,
    "Heating Oil": 263e-6,
    "Electricity": 295.1e-6,
    "Bulk LPG": 229e-6,
    "Wood Pellets (bags)": 390e-6,
    "Wood Pellets (bulk)": 160e-6,
    "Solid Multi-Fuel": 390e-6,
    "Manuf.Smokeless Fuel": 390e-6,
    "Bottled LPG": 229e-6,
    "House Coal": 340e-6,
    "Wood Logs": 390e-6,
    "Peat Briquettes": 355e-6,
    "Anthracite": 340e-6,
}

with Flow("Extract infrastructure small area line lengths") as flow:
    create_folder_structure = tasks.create_folder_structure(DATA_DIR)
    check_data_exists = tasks.check_file_exists.map(
        list(INPUT_FILEPATHS.values())
    ).set_upstream(create_folder_structure)

    kwh_to_mwh = 1e-3

    # from SEAI's conversion factors
    electricity_mwh_to_tco2 = 0.2951
    gas_mwh_to_tco2 = 0.2047

    # from Eirgrid 2020 Tomorrow's Energy Scenarios
    data_centre_mwh = 32500000
    data_centre_tco2 = 10541700

    # adapted from NTA's 2021 Model
    dart_mwh = 24377.70008318234
    luas_mwh = 23482.82576549033
    commuter_mwh = 67392.17766573868
    intercity_mwh = 15340.77429127268
    rail_mwh = dart_mwh + luas_mwh + commuter_mwh + intercity_mwh
    dart_tco2 = 7910.563680339676
    luas_tco2 = 7620.176962354371
    commuter_tco2 = 17784.795687928676
    intercity_tco2 = 4048.4303359085247
    rail_tco2 = dart_tco2 + luas_tco2 + commuter_tco2 + intercity_tco2

    road_mwh = 7308683.426581341
    road_tco2 = 1727902.3409237664

    # from SEAI's Monitoring & Reporting
    public_sector_buildings = tasks.read_csv(
        INPUT_FILEPATHS["public_sector_buildings"]
    ).set_upstream(check_data_exists)
    public_sector_electricity_mwh = (
        tasks.sum_column(public_sector_buildings, "electricity_kwh_2018") * kwh_to_mwh
    )
    public_sector_gas_mwh = (
        tasks.sum_column(public_sector_buildings, "gas_kwh_2018") * kwh_to_mwh
    )
    public_sector_tco2 = (
        public_sector_electricity_mwh * electricity_mwh_to_tco2
        + public_sector_gas_mwh * gas_mwh_to_tco2
    )
    #  public_sector_heat_mwh

    # from SEAI's Small Area BERs
    residential_buildings = tasks.read_parquet(
        INPUT_FILEPATHS["residential_buildings"]
    ).set_upstream(check_data_exists)

    # assume electricity ~= 5 * (pump_fan_demand + lighting_demand)
    # ... SEAI's 2013 Energy in the Residential Sector estimates lighting is 16%
    # and pumps/fans are 4%
    lighting_and_pumps_to_demand = 5
    residential_electricity_mwh = (
        (
            tasks.sum_column(residential_buildings, "pump_fan_demand")
            + tasks.sum_column(residential_buildings, "lighting_demand")
        )
        * lighting_and_pumps_to_demand
        * kwh_to_mwh
    )
    residential_heat_mwh = (
        tasks.sum_column(residential_buildings, "main_sh_demand")
        + tasks.sum_column(residential_buildings, "suppl_sh_demand")
        + tasks.sum_column(residential_buildings, "main_hw_demand")
        + tasks.sum_column(residential_buildings, "suppl_hw_demand")
    ) * kwh_to_mwh
    residential_tco2 = tasks.estimate_residential_emissions(
        bers=residential_buildings,
        electricity_mwh=residential_electricity_mwh,
        fuels_to_emission_factors=FUELS_TO_EMISSION_FACTORS_KWH_TO_TCO2,
        heating_columns=[
            "main_sh_demand",
            "suppl_sh_demand",
            "main_hw_demand",
            "suppl_hw_demand",
        ],
    )

    # from Valuation Office
    commercial_buildings = tasks.read_csv(
        INPUT_FILEPATHS["commercial_buildings"]
    ).set_upstream(check_data_exists)
    commercial_electricity_mwh = (
        tasks.sum_column(commercial_buildings, "electricity_demand_kwh_per_y")
        * kwh_to_mwh
    )
    commercial_gas_mwh = (
        tasks.sum_column(commercial_buildings, "fossil_fuel_demand_kwh_per_y")
        * kwh_to_mwh
    )
    commercial_heat_mwh = (
        tasks.sum_column(commercial_buildings, "heat_demand_kwh_per_y") * kwh_to_mwh
    )
    commercial_tco2 = (
        commercial_electricity_mwh * electricity_mwh_to_tco2
        + commercial_gas_mwh * gas_mwh_to_tco2
    )

    transport_tco2 = road_tco2 + rail_tco2
    tco2 = tasks.create_series(
        {
            "transport": transport_tco2,
            "municipal": public_sector_tco2,
            "residential": residential_tco2,
            "commercial": commercial_tco2,
            "data_centres": data_centre_tco2,
        }
    )
    tasks.plot_pie(tco2, filepath=OUTPUT_FILEPATHS["plot"]["tco2"])
    tasks.save_to_csv(tco2, filepath=OUTPUT_FILEPATHS["data"]["tco2"], index=True)

    transport_mwh = road_mwh + rail_mwh
    mwh = tasks.create_series(
        {
            "transport": transport_mwh,
            "municipal": public_sector_electricity_mwh + public_sector_gas_mwh,
            "residential": residential_electricity_mwh + residential_heat_mwh,
            "commercial": commercial_electricity_mwh + commercial_gas_mwh,
            "data_centres": data_centre_mwh,
        }
    )
    tasks.plot_pie(mwh, filepath=OUTPUT_FILEPATHS["plot"]["mwh"])
    tasks.save_to_csv(mwh, filepath=OUTPUT_FILEPATHS["data"]["mwh"], index=True)


state = flow.run()

flow.visualize(flow_state=state, filename=HERE / "flow", format="png")
