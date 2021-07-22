import os

import dotenv

dotenv.load_dotenv(".prefect")  # load local prefect configuration prior to import!
import prefect

from globals import CONFIG
from globals import DATA_DIR
import tasks


dotenv.load_dotenv()  # load s3 credentials
tasks.check_if_s3_keys_are_defined()

with prefect.Flow("Estimate Heat Demand Density") as flow:
    # Set CONFIG
    assumed_boiler_efficiency = prefect.Parameter(
        "Assumed Boiler Efficiency", default=0.9
    )

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
    non_residential_demand = tasks.apply_benchmarks_to_valuation_office_floor_areas(
        valuation_office=valuation_office_map,
        benchmark_uses=benchmark_uses,
        benchmarks=benchmarks,
        assumed_boiler_efficiency=assumed_boiler_efficiency,
    )
    clean_bers = tasks.drop_small_areas_not_in_boundaries(
        bers=raw_bers, small_area_boundaries=small_area_boundaries
    )
    residential_demand = tasks.extract_residential_heat_demand(raw_bers)
    demand_mwh_per_y = tasks.amalgamate_heat_demands_to_small_areas(
        residential=residential_demand, non_residential=non_residential_demand
    )
    demand_tj_per_km2 = tasks.convert_from_mwh_per_y_to_tj_per_km2(
        demand=demand_mwh_per_y, small_area_boundaries=small_area_boundaries
    )

    # Map Demand
    boundaries = tasks.link_small_areas_to_local_authorities(
        small_area_boundaries=small_area_boundaries,
        local_authority_boundaries=local_authority_boundaries,
    )
    tasks.map_demand(
        demand=demand_tj_per_km2,
        boundaries=boundaries,
        filepath=DATA_DIR / "processed" / "dublin_small_area_demand_tj_per_km2.geojson",
    )


flow.run()
