from dotenv import load_dotenv

load_dotenv(".prefect")
from prefect import Flow

import tasks
from globals import DATA_DIR
from globals import HERE

URLS = {
    "grid": "https://codema-dev.s3.eu-west-1.amazonaws.com/heatmap-download-version-nov-2020.csv",
    "small_area_boundaries": "https://codema-dev.s3.eu-west-1.amazonaws.com/dublin_small_area_boundaries_in_routing_keys.gpkg",
}

OUTPUT_FILEPATHS = {
    "small_area_lv_capacity": DATA_DIR / "processed" / "small_area_lv_capacity.csv",
    "lv_substations_in_small_areas": DATA_DIR
    / "processed"
    / "lv_substations_in_small_areas.gpkg",
}

with Flow("Estimate LV capacity") as flow:
    create_folder_structure = tasks.create_folder_structure(DATA_DIR)
    raw_substations = tasks.load_esb_substation_data(URLS["grid"]).set_upstream(
        create_folder_structure
    )
    small_area_boundaries = tasks.load_small_area_boundaries(
        URLS["small_area_boundaries"]
    ).set_upstream(create_folder_structure)

    substations = tasks.convert_to_geodataframe(
        raw_substations,
        x="Longitude",
        y="Latitude",
        from_crs="EPSG:4326",
        to_crs="EPSG:2157",
    )
    lv_substations = tasks.query(substations, "`Voltage Class` == 'LV'")
    lv_substations_in_small_areas = tasks.link_to_small_area_boundaries(
        left_df=lv_substations, right_df=small_area_boundaries, op="within"
    )
    small_area_lv_capacity = tasks.amalgamate_to_small_areas(
        lv_substations_in_small_areas,
        by=["small_area"],
        on_columns=["Installed Capacity MVA", "Demand Available MVA"],
    )

    tasks.save_to_csv(
        small_area_lv_capacity, filepath=OUTPUT_FILEPATHS["small_area_lv_capacity"]
    )
    tasks.save_to_gpkg(
        lv_substations_in_small_areas,
        filepath=OUTPUT_FILEPATHS["lv_substations_in_small_areas"],
    )

state = flow.run()
flow.visualize(flow_state=state, filename=HERE / "flow", format="png")
