import pandas as pd
import prefect

import functions


load_residential = prefect.task(
    functions.load_parquet, name="Load Residential Buildings"
)
load_commercial = prefect.task(functions.load_parquet, name="Load Commercial Buildings")
load_municipal = prefect.task(functions.load_parquet, name="Load Municipal Buildings")

check_file_exists = prefect.task(functions.check_file_exists)
create_folder_structure = prefect.task(functions.create_folder_structure)
read_csv = prefect.task(pd.read_csv)
read_parquet = prefect.task(pd.read_parquet)
read_json = prefect.task(pd.read_json)

sum_column = prefect.task(lambda df, c: float(df[c].sum()), name="Sum Column")
estimate_residential_emissions = prefect.task(functions.estimate_residential_emissions)
create_series = prefect.task(pd.Series)
plot_pie = prefect.task(functions.plot_pie)

save_to_csv = prefect.task(
    lambda df, filepath, index: df.to_csv(filepath, index=index), name="Save to CSV"
)
