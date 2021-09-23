from typing import Any

import pandas as pd


def concatenate_dublin_postal_districts_and_county_dublin(
    upstream: Any, product: Any
) -> None:
    county = pd.read_csv(
        upstream["download_county_residential_networked_gas_consumption"], index_col=0
    )
    postal_district = pd.read_csv(
        upstream[
            "download_dublin_postal_district_residential_networked_gas_consumption"
        ],
        index_col=0,
    )
    dublin = pd.concat([county.loc[["Dublin County"], :], postal_district])
    dublin.to_csv(product)
