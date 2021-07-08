from pathlib import Path

import pandas as pd

DATA_DIR = Path("../data")

deap_age_bands = {
    "A": "before 1900",
    "B": "1900 - 1929",
    "C": "1930 - 1949",
    "D": "1950 - 1966",
    "E": "1967 - 1977",
    "F": "1978 - 1982",
    "G": "1983 - 1993",
    "H": "1994 - 1999",
    "I": "2000 - 2004",
    "J": "2005 - 2009",
    "K": "2010 onwards",
}  # age_band : regulatory_period

regulatory_period_to_cso_period = {
    "before 1900": "before 1919",
    "1900 - 1929": "before 1919",
    "1930 - 1949": "1919 - 1945",
    "1950 - 1966": "1946 - 1960",
    "1967 - 1977": "1971 - 1980",
    "1978 - 1982": "1981 - 1990",
    "1983 - 1993": "1981 - 1990",
    "1994 - 1999": "1991 - 2000",
    "2000 - 2004": "2001 - 2005",
    "2005 - 2009": "2005 - 2010",
    "2010 onwards": "2011 or later",
}

deap_default_wall_uvalues = (
    pd.read_csv(DATA_DIR / "deap-default-wall-uvalues.csv", index_col=0)
    .rename(columns=deap_age_bands)
    .rename(columns=regulatory_period_to_cso_period)
    .assign(**{"1961 - 1970": lambda df: df["1946 - 1960"]})
    .loc[:, lambda df: ~df.columns.duplicated()]
    .stack()
    .reset_index()
    .rename(columns={"level_1": "period_built", 0: "uvalue_wall"})
)

deap_default_wall_uvalues.to_csv(
    DATA_DIR / "deap-default-wall-uvalues-by-period-built.csv", index=False
)
