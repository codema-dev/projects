import dotenv

dotenv.load_dotenv(".prefect")  # load local prefect configuration prior to import!
import prefect

from globals import BASE_DIR
from globals import DATA_DIR
import tasks


dotenv.load_dotenv()  # load s3 credentials
tasks.check_if_s3_keys_are_defined()

URLS = {
    "benchmarks": "s3://codema-dev/benchmarks.csv",
    "benchmark_uses": "s3://codema-dev/benchmarks.zip",
    "bers": "s3://codema-dev/small_area_bers.parquet",
    "small_area_boundaries": "s3://codema-dev/dublin_small_area_boundaries_in_routing_keys.gpkg",
    "valuation_office": "s3://codema-dev/valuation_office_dublin_april_2021.parquet",
}

OUTPUT_FILEPATHS = {
    "valuation_office_demands": DATA_DIR / "processed" / "valuation_office_demands.csv"
}

with prefect.Flow("Estimate Heat Demand Density") as flow:
    # Set CONFIG
    assumed_boiler_efficiency = prefect.Parameter(
        "Assumed Boiler Efficiency", default=0.85
    )

    # Extract
    valuation_office = tasks.load_valuation_office(url=URLS["valuation_office"])
    benchmark_uses = tasks.load_benchmark_uses(
        url=URLS["benchmark_uses"], filesystem_name="s3"
    )
    benchmarks = tasks.load_benchmarks(url=URLS["benchmarks"])
    small_area_boundaries = tasks.load_small_area_boundaries(
        url=URLS["small_area_boundaries"]
    )

    # Estimate Demand
    valuation_office_map = tasks.link_valuation_office_to_small_areas(
        valuation_office=valuation_office,
        small_area_boundaries=small_area_boundaries,
    )
    with_benchmarks = tasks.link_valuation_office_to_benchmarks(
        valuation_office=valuation_office_map,
        benchmark_uses=benchmark_uses,
        benchmarks=benchmarks,
        valuation_office_use_column="Use1",
        use_column="use",
        benchmark_column="Benchmark",
    )
    with_fossil_fuel_demand = tasks.apply_benchmarks_to_valuation_office_floor_areas(
        with_benchmarks,
        demand_column="fossil_fuel_demand_kwh_per_y",
        energy_benchmark_column="Typical fossil fuel [kWh/m²y]",
        floor_area_column="Total_SQM",
    )
    with_electricity_demand = tasks.apply_benchmarks_to_valuation_office_floor_areas(
        with_benchmarks,
        demand_column="electricity_demand_kwh_per_y",
        energy_benchmark_column="Typical electricity [kWh/m²y]",
        floor_area_column="Total_SQM",
    )
    with_heat_demand = tasks.estimate_heat_demand(
        with_electricity_demand,
        heat_demand_column="heat_demand_kwh_per_y",
        fossil_fuel_benchmark_column="Typical fossil fuel [kWh/m²y]",
        floor_area_column="Total_SQM",
        industrial_space_heat_column="Industrial space heat [kWh/m²y]",
        assumed_boiler_efficiency=assumed_boiler_efficiency,
    )

    tasks.to_csv(with_heat_demand, OUTPUT_FILEPATHS["valuation_office_demands"])

flow.run()
