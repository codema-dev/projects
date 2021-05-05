from pathlib import Path

import pandas as pd

from dublin_building_stock.residential import (
    create_dublin_ber_public,
)

data_dir = Path("../data")

dublin_ber_public = create_dublin_ber_public(data_dir / "BERPublicsearch_parquet")
dublin_ber_public.to_parquet(data_dir / "dublin_ber_public.parquet")