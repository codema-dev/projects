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

with prefect.Flow("Combine MPRNs with GPRNs") as flow:
    mprn_sheet_name = prefect.Parameter("MPRN Sheet Name", default="MPRN_data")
    gprn_sheet_name = prefect.Parameter("GPRN Sheet Name", default="GPRN_data")
    sheets = tasks.load_monitoring_and_reporting(
        url=CONFIG["monitoring_and_reporting"]["url"]
    )
    mprn = tasks.clean_mprn(
        dfs=sheets, sheet_name=mprn_sheet_name, demand_type="electricity"
    )
    gprn = tasks.clean_gprn(dfs=sheets, sheet_name=gprn_sheet_name, demand_type="gas")
    raw_m_and_r = tasks.merge(mprn, gprn)
    m_and_r = tasks.pivot_to_one_column_per_year(raw_m_and_r)

from prefect.utilities.debug import raise_on_exception

with raise_on_exception():
    flow.run()
