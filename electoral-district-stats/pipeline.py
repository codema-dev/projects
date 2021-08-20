from os import getenv

from dotenv import load_dotenv

load_dotenv(".prefect")
from prefect import Flow

import tasks
from globals import DATA_DIR
from globals import HERE

URLS = {
    "commercial": "s3://codema-dev/valuation_office_dublin_april_2021.parquet",
    "residential": "s3://codema-dev/bers_dublin_june_2021.parquet",
    "public_sector": "s3://codema-dev/monitoring_and_reporting_dublin_21_1_20.parquet",
    "small_area_boundaries": "s3://codema-dev/dublin_small_area_boundaries_in_routing_keys.gpkg",
}

INPUT_FILEPATHS = {
    "commercial": DATA_DIR / "external" / "valuation_office_dublin_april_2021.csv",
    "residential": DATA_DIR / "external" / "bers_dublin_june_2021.csv",
    "public_sector": DATA_DIR
    / "external"
    / "monitoring_and_reporting_dublin_21_1_20.csv",
    "small_area_boundaries": DATA_DIR
    / "external"
    / "dublin_small_area_boundaries_in_routing_keys.gpkg",
}

OUTPUT_FILEPATHS = {
    "commercial_floor_areas": DATA_DIR
    / "processed"
    / "commercial_in_electoral_districts_dublin_april_2021.csv",
    "commercial": DATA_DIR
    / "processed"
    / "commercial_floor_areas_in_electoral_districts_dublin_april_2021.csv",
    "residential": DATA_DIR
    / "processed"
    / "residential_in_electoral_districts_dublin_june_2021.csv",
    "public_sector": DATA_DIR
    / "processed"
    / "public_sector_in_electoral_districts_dublin_21_1_20.csv",
}

load_dotenv(".env")
message = f"""

    Please create a .env file
    
    In this directory: {HERE.resolve()}

    With the following contents:
    
    AWS_ACCESS_KEY_ID=YOUR_KEY
    AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
"""
assert getenv("AWS_ACCESS_KEY_ID") is not None, message
assert getenv("AWS_SECRET_ACCESS_KEY") is not None, message

with Flow("Amalgamate Buildings to Electoral Districts") as flow:
    create_folder_structure = tasks.create_folder_structure(DATA_DIR)
    commercial = tasks.load_commercial(
        URLS["commercial"], INPUT_FILEPATHS["commercial"]
    )
    raw_public_sector = tasks.load_public_sector(
        URLS["public_sector"], INPUT_FILEPATHS["public_sector"]
    )
    residential = tasks.load_residential(
        URLS["residential"], INPUT_FILEPATHS["residential"]
    )
    small_area_boundaries = tasks.load_small_area_boundaries(
        URLS["small_area_boundaries"], INPUT_FILEPATHS["small_area_boundaries"]
    )

    public_sector = tasks.convert_to_geodataframe(
        raw_public_sector,
        x="longitude",
        y="latitude",
        from_crs="EPSG:4326",
        to_crs="EPSG:2157",
    )
    small_areas_to_electoral_districts = tasks.extract_columns(
        small_area_boundaries, columns=["small_area", "cso_ed_id"]
    )
    public_sector_with_eds = tasks.sjoin(
        public_sector, small_area_boundaries, op="within"
    )

    electoral_district_commercial_floor_areas = tasks.amalgamate_to_electoral_district(
        commercial,
        granularity="cso_ed_id",
        columns="Benchmark",
        on="Total_SQM",
    )
    electoral_district_commercial = tasks.count_in_electoral_district(
        commercial,
        granularity="cso_ed_id",
        columns="Benchmark",
    )
    electoral_district_residential = tasks.count_in_electoral_district(
        residential,
        granularity="cso_ed_id",
        columns="period_built",
    )
    electoral_district_public_sector = tasks.count_in_electoral_district(
        public_sector_with_eds,
        granularity="cso_ed_id",
        columns="category",
    )

    tasks.save_to_csv(
        electoral_district_commercial_floor_areas,
        OUTPUT_FILEPATHS["commercial_floor_areas"],
    )
    tasks.save_to_csv(electoral_district_commercial, OUTPUT_FILEPATHS["commercial"])
    tasks.save_to_csv(electoral_district_residential, OUTPUT_FILEPATHS["residential"])
    tasks.save_to_csv(
        electoral_district_public_sector, OUTPUT_FILEPATHS["public_sector"]
    )

    # Manually specify missing links
    commercial.set_upstream(create_folder_structure)
    public_sector.set_upstream(create_folder_structure)
    residential.set_upstream(create_folder_structure)
    small_area_boundaries.set_upstream(create_folder_structure)


state = flow.run()
flow.visualize(flow_state=state, filename=HERE / "flow", format="png")
