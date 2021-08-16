import dotenv

dotenv.load_dotenv(".prefect")  # load local prefect configuration prior to import!
import prefect

from globals import BASE_DIR
from globals import CONFIG
from globals import DATA_DIR
import tasks


dotenv.load_dotenv()  # load s3 credentials
tasks.check_if_s3_keys_are_defined()

filepaths = {
    "data": {
        "demand_map": DATA_DIR
        / "processed"
        / "dublin_small_area_emissions_tco2_per_y.geojson",
    },
    "pynb": {
        "demand_map": BASE_DIR / "map_emissions.py",
    },
    "ipynb": {"demand_map": DATA_DIR / "notebooks" / "map_emissions.ipynb"},
}


with prefect.Flow("Estimate Heat Demand Density") as flow:

    # Extract
    valuation_office = tasks.load_valuation_office(
        url=CONFIG["valuation_office"]["url"]
    )
    raw_bers = tasks.load_bers(url=CONFIG["bers"]["url"])
    benchmark_uses = tasks.load_benchmark_uses(
        url=CONFIG["benchmark_uses"]["url"], filesystem_name="s3"
    )
    benchmarks = tasks.load_benchmarks(url=CONFIG["benchmarks"]["url"])
    small_area_boundaries = tasks.load_small_area_boundaries(
        url=CONFIG["small_area_boundaries"]["url"]
    )
    local_authority_boundaries = tasks.load_local_authority_boundaries(
        url=CONFIG["local_authority_boundaries"]["url"]
    )

    # Estimate Demand
    valuation_office_map = tasks.link_valuation_office_to_small_areas(
        valuation_office=valuation_office,
        small_area_boundaries=small_area_boundaries,
    )
    non_residential_emissions = tasks.extract_non_residential_emissions(
        valuation_office=valuation_office_map,
        benchmark_uses=benchmark_uses,
        benchmarks=benchmarks,
    )
    clean_bers = tasks.drop_small_areas_not_in_boundaries(
        bers=raw_bers, small_area_boundaries=small_area_boundaries
    )
    residential_emissions = tasks.extract_residential_emissions(clean_bers)
    emissions_tco2_per_y = tasks.amalgamate_emissions_to_small_areas(
        residential=residential_emissions, non_residential=non_residential_emissions
    )

    # Convert to Map
    boundaries = tasks.link_small_areas_to_local_authorities(
        small_area_boundaries=small_area_boundaries,
        local_authority_boundaries=local_authority_boundaries,
    )
    demand_map = tasks.link_emissions_to_boundaries(
        demands=emissions_tco2_per_y,
        boundaries=boundaries,
    )

    # Plot
    save_demand_map = tasks.save_demand_map(
        demand_map=demand_map,
        filepath=filepaths["data"]["demand_map"],
    )
    convert_plotting_script_to_ipynb = tasks.convert_plotting_script_to_ipynb(
        input_filepath=filepaths["pynb"]["demand_map"],
        output_filepath=filepaths["ipynb"]["demand_map"],
        fmt="py:light",
    )
    execute_hdd_plot_ipynb = tasks.execute_plot_ipynb(
        path=filepaths["ipynb"]["demand_map"],
        parameters={
            "SAVE_PLOTS": True,
            "DATA_DIR": str(DATA_DIR),
            "demand_map_filepath": str(filepaths["data"]["demand_map"]),
        },
    )

    # Manually set dependencies where no inputs are passed between tasks
    convert_plotting_script_to_ipynb.set_upstream(save_demand_map)
    execute_hdd_plot_ipynb.set_upstream(convert_plotting_script_to_ipynb)

flow.run()
