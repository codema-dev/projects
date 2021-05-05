from pathlib import Path

import pandas as pd

from dublin_building_stock.residential import (
    create_dublin_ber_private,
)

data_dir = Path("../data")

small_areas_2011_vs_2016 = pd.read_csv(data_dir / "small_areas_2011_vs_2016.csv")
dublin_ber_private = create_dublin_ber_private(data_dir, small_areas_2011_vs_2016)
dublin_ber_private.to_parquet(data_dir / "dublin_ber_private.parquet")
