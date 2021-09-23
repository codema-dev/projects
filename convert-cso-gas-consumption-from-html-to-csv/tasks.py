from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def _replace_header_with_second_row(df: pd.DataFrame) -> pd.DataFrame:
    """Convert from:
    |    | 0                                                                                    |
    |---:|:-------------------------------------------------------------------------------------|
    |  0 | Table 6C Number of Meters by Dublin Postal District for Residential Sector 2011-2020 |
    |  1 | Number of Meters                                                                     |
    |  2 | Dublin Postal District                                                               |
    |  3 | Dublin 01                                                                            |

    To:
    |    | Dublin Postal District   |
    |---:|:-------------------------|
    |  0 | Dublin 01                |
    """
    return (
        pd.DataFrame(df.to_numpy()[2:])
        .rename(columns=df.iloc[2])
        .drop(0)
        .reset_index(drop=True)
    )


def _drop_rows_after_first_empty_row(df: pd.DataFrame) -> pd.DataFrame:
    """Convert from:
    |     | Dublin Postal District   |
    |----:|:-------------------------|
    |  0  | Dublin 01                |
    | ... | ...                      |
    | 22  | Dublin 24                |
    | 23  | nan                      |
    | 24  | Total                    |

    To:
    |     | Dublin Postal District   |
    |----:|:-------------------------|
    |  0  | Dublin 01                |
    | ... | ...                      |
    | 22  | Dublin 24                |
    """
    is_row_empty = df.iloc[:, 0].isnull()
    try:
        # subtract one so empty row also not included when indexing with iloc!
        index_of_first_empty_row = df[is_row_empty == True].index[0] - 1
    except IndexError:
        index_of_first_empty_row = len(df)
    return df.iloc[:index_of_first_empty_row].copy()


def _replace_dash_with_nan(df: pd.DataFrame) -> pd.DataFrame:
    """Convert from:
    |     | County                  | 2011   |
    |----:|:------------------------|:-------|
    |  0  | Carlow                  | 6610   |
    | ... | ...                     | ...    |
    |  8  | Kerry                   | –      |

    To:
    |     | County                  | 2011   |
    |----:|:------------------------|:-------|
    |  0  | Carlow                  | 6610   |
    | ... | ...                     | ...    |
    |  8  | Kerry                   |        |
    """
    return df.replace("–", np.nan).copy()


def _set_first_column_as_index(df: pd.DataFrame) -> pd.DataFrame:
    """Convert from:
    |    | County                  | 2011   |
    |---:|:------------------------|:-------|
    |  1 | Carlow                  | 6610   |
    |  2 | Cavan                   | 1397   |

    To:
    | County                  | 2011   |
    |:------------------------|:-------|
    | Carlow                  | 6610   |
    | Cavan                   | 1397   |
    """
    return df.set_index(df.columns[0]).copy()


def _clean_table(df: pd.DataFrame) -> pd.DataFrame:
    with_correct_header = _replace_header_with_second_row(df)
    with_empty_row_downwards_dropped = _drop_rows_after_first_empty_row(
        with_correct_header
    )
    with_dashes_replaced = _replace_dash_with_nan(with_empty_row_downwards_dropped)
    with_first_column_as_index = _set_first_column_as_index(with_dashes_replaced)
    return with_first_column_as_index


def convert_html_to_tables(upstream: Any, product: Any) -> None:
    raw_tables = pd.read_html(upstream["download_cso_networkedgasconsumption2020_html"])
    dirpath = Path(product)
    dirpath.mkdir(exist_ok=True)

    raw_consumption_tables = [t for t in raw_tables if "Table" in str(t.iloc[0, 0])]
    tables = {t.iloc[0, 0]: _clean_table(t) for t in raw_consumption_tables}
    for key, table in tables.items():
        filename = key + ".csv"
        filepath = Path(product) / filename
        table.to_csv(filepath)
