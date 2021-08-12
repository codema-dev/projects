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
        / "dublin_small_area_demand_tj_per_km2.geojson",
    },
    "pynb": {
        "demand_map": BASE_DIR / "map_heat_demand_densities.py",
    },
    "ipynb": {"demand_map": DATA_DIR / "notebooks" / "map_heat_demand_densities.ipynb"},
}


with prefect.Flow("Estimate Heat Demand Density") as flow:
    # Set CONFIG
    assumed_boiler_efficiency = prefect.Parameter(
        "Assumed Boiler Efficiency", default=0.9
    )

    # Extract
    valuation_office = tasks.load_valuation_office(
        url=CONFIG["valuation_office"]["url"]
    )
    bers = tasks.load_bers(url=CONFIG["bers"]["url"])
    benchmark_uses = tasks.load_benchmark_uses(
        url=CONFIG["benchmark_uses"]["url"], filesystem_name="s3"
    )
    benchmarks = tasks.load_benchmarks(url=CONFIG["benchmarks"]["url"])
    small_area_boundaries = tasks.load_small_area_boundaries(
        url=CONFIG["small_area_boundaries"]["url"]
    )

    # Estimate Demand
    valuation_office_map = tasks.link_valuation_office_to_small_areas(
        valuation_office=valuation_office,
        small_area_boundaries=small_area_boundaries,
    )
    non_residential_demand = tasks.apply_benchmarks_to_valuation_office_floor_areas(
        valuation_office=valuation_office_map,
        benchmark_uses=benchmark_uses,
        benchmarks=benchmarks,
        assumed_boiler_efficiency=assumed_boiler_efficiency,
    )
    residential_demand = tasks.extract_residential_heat_demand(bers)
    demand_mwh_per_y = tasks.amalgamate_heat_demands_to_small_areas(
        residential=residential_demand, non_residential=non_residential_demand
    )
    demand_tj_per_km2 = tasks.convert_from_mwh_per_y_to_tj_per_km2(
        demand=demand_mwh_per_y, small_area_boundaries=small_area_boundaries
    )

    # Convert to Map
    demand_map = tasks.link_demands_to_boundaries(
        demands=demand_tj_per_km2,
        boundaries=small_area_boundaries,
    )

    # Plot
    save_demand_map = tasks.save_demand_map(
        demand_map=demand_map,
        filepath=filepaths["data"]["demand_map"],
    )
    convert_heat_demand_density_plotting_script_to_ipynb = (
        tasks.convert_heat_demand_density_plotting_script_to_ipynb(
            input_filepath=filepaths["pynb"]["demand_map"],
            output_filepath=filepaths["ipynb"]["demand_map"],
            fmt="py:light",
        )
    )
    execute_hdd_plot_ipynb = tasks.execute_hdd_plot_ipynb(
        path=filepaths["ipynb"]["demand_map"],
        parameters={
            "SAVE_PLOTS": True,
            "DATA_DIR": str(DATA_DIR),
            "demand_map_filepath": str(filepaths["data"]["demand_map"]),
        },
    )

    # Manually set dependencies where no inputs are passed between tasks
    convert_heat_demand_density_plotting_script_to_ipynb.set_upstream(save_demand_map)
    execute_hdd_plot_ipynb.set_upstream(
        convert_heat_demand_density_plotting_script_to_ipynb
    )

flow.run()
