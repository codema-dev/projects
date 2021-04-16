from collections import defaultdict
from os import getenv

from dotenv import load_dotenv
import geopandas as gpd
from loguru import logger
import numpy as np
import pandas as pd
from postal.expand import expand_address

from dublin_building_stock.spatial_operations import (
    get_geometries_within,
    convert_to_geodataframe,
)


# loads environmental variables from a .env file (such as API keys for Google Maps)
load_dotenv()


def load_uses_benchmarks(data_dir):
    dirpath = data_dir / "benchmarks" / "uses"
    benchmark_uses = defaultdict()
    for filepath in dirpath.glob("*.txt"):
        with open(filepath, "r") as file:
            benchmark_uses[filepath.stem] = [line.rstrip() for line in file]
    return {i: k for k, v in benchmark_uses.items() for i in v}


def load_benchmarks(data_dir):

    return pd.read_excel(data_dir / "benchmarks" / "benchmarks.xlsx").dropna(
        how="all", axis="columns"
    )


def create_valuation_office_public(
    data_dir, uses_linked_to_benchmarks, benchmarks, small_area_boundaries
):
    vo_data_dir = data_dir / "valuation_office"
    vo_public_raw = pd.concat(
        [pd.read_csv(filepath) for filepath in vo_data_dir.glob("*.csv")]
    ).reset_index(drop=True)
    benchmark_columns = [
        "Benchmark",
        "Typical fossil fuel [kWh/m²y]",
        "Industrial space heat [kWh/m²y]",
        "Typical Area [m²]",
        "Area Upper Bound [m²]",
        "GIA to Sales",
    ]
    kwh_to_mwh_conversion_factor = 10 ** -3
    vo_public_clean = (
        vo_public_raw.pipe(
            convert_to_geodataframe, x=" X ITM", y=" Y ITM", crs="EPSG:2157"
        )
        .join(
            vo_public_raw["Uses"].str.split(", ", expand=True)
        )  # Split 'USE, -' into 'USE', '-'
        .drop(columns=[2, 3])  # both are empty columns
        .rename(columns={0: "use_1", 1: "use_2", "Property Number": "ID"})
        .assign(
            # Benchmark=lambda gdf: gdf["use_1"].map(uses_linked_to_benchmarks),
            benchmark_1=lambda gdf: gdf["use_1"]
            .map(uses_linked_to_benchmarks)
            .astype(str),
            benchmark_2=lambda gdf: gdf["use_2"]
            .map(uses_linked_to_benchmarks)
            .astype(str),
            ID=lambda df: df["ID"].astype("int32"),
        )  # link uses to benchmarks so can merge on common benchmarks
        # .merge(vo_benchmarks, how="left")
        .merge(
            benchmarks[benchmark_columns],
            how="left",
            left_on="benchmark_1",
            right_on="Benchmark",
        )
        .assign(
            bounded_area_m2=lambda df: np.where(
                (df["Area"] > 5) & (df["Area"] < df["Area Upper Bound [m²]"]),
                df["Area"],
                np.nan,
            ),  # Remove all areas outside of 5 <= area <= Upper Bound
            inferred_area_m2=lambda df: np.round(
                df["bounded_area_m2"].fillna(df["Typical Area [m²]"])
                * df["GIA to Sales"].fillna(1)
            ),
            area_is_estimated=lambda df: df["bounded_area_m2"].isnull(),
            heating_mwh_per_year=lambda df: np.round(
                (
                    df["Typical fossil fuel [kWh/m²y]"].fillna(0)
                    * df["inferred_area_m2"]
                    + df["Industrial space heat [kWh/m²y]"].fillna(0)
                    * df["inferred_area_m2"]
                )
                * kwh_to_mwh_conversion_factor
            ),
        )
        .pipe(gpd.sjoin, small_area_boundaries[["SMALL_AREA", "geometry"]], op="within")
        .drop(columns="index_right")
    )
    vo_public_clean.to_file(data_dir / "valuation_office_public.gpkg", driver="GPKG")


def _load_dcc_vo_private(data_dir):
    return (
        pd.read_excel(
            data_dir / "Valuation-Office-2015" / "DCC.xlsx",
            sheet_name="Energy Calculation Sheet",
            header=3,
        )
        .assign(ID=lambda df: df["ID"].astype(str).str.extract("(\d{3,})")[0])
        .dropna(how="all", axis="rows", subset=["ID"])
        .dropna(how="all", axis="columns")
        .pipe(convert_to_geodataframe, x="X_Long", y="Y_Lat", crs="EPSG:2157")
    )


def _load_dlrcc_vo_private(data_dir):
    return (
        pd.read_excel(
            data_dir / "Valuation-Office-2015" / "DLRCC.xlsm",
            sheet_name="Energy Demand Calculation",
            header=3,
        )
        .assign(ID=lambda df: df["ID"].astype(str).str.extract("(\d+)")[0])
        .dropna(how="all", axis="rows", subset=["ID"])
        .dropna(how="all", axis="columns")
        .pipe(convert_to_geodataframe, x="X_Long", y="Y_Lat", crs="EPSG:2157")
    )


def _load_sdcc_vo_private(data_dir):
    return (
        pd.read_excel(
            data_dir / "Valuation-Office-2015" / "SDCC.xlsx",
            sheet_name="Energy Calculation Sheet",
            header=3,
        )
        .assign(ID=lambda df: df["ID"].astype(str).str.extract("(\d{3,})")[0])
        .dropna(how="all", axis="rows", subset=["ID"])
        .dropna(how="all", axis="columns")
        .pipe(convert_to_geodataframe, x="X_Long", y="Y_Lat", crs="EPSG:29903")
        .to_crs(epsg=2157)
    )


def _load_fcc_vo_private(data_dir):
    return (
        pd.read_excel(
            data_dir / "Valuation-Office-2015" / "FCC.xlsm",
            sheet_name="Energy Demand Calculation",
            header=3,
        )
        .assign(ID=lambda df: df["ID"].astype(str).str.extract("(\d{3,})")[0])
        .dropna(how="all", axis="rows", subset=["ID"])
        .dropna(how="all", axis="columns")
        .pipe(convert_to_geodataframe, x="X_Long", y="Y_Lat", crs="EPSG:2157")
    )


def create_valuation_office_private(
    data_dir, uses_linked_to_benchmarks, benchmarks, small_area_boundaries
):
    dcc_vo_private = _load_dcc_vo_private(data_dir)
    dlrcc_vo_private = _load_dlrcc_vo_private(data_dir)
    sdcc_vo_private = _load_sdcc_vo_private(data_dir)
    fcc_vo_private = _load_fcc_vo_private(data_dir)

    use_columns = [
        "ID",
        "Benchmark",
        "Property Use",
        "inferred_area_m2",
        "area_is_estimated",
        "Industrial",
        "heating_mwh_per_year",
        "Typical Area [m²]",
        "area_conversion_factors",
        "Area (m2)",
        "typical_ff",
        "industrial_sh",
        "latitude",
        "longitude",
        "SMALL_AREA",
        "geometry",
    ]
    valuation_office_private = (
        pd.concat([dcc_vo_private, dlrcc_vo_private, sdcc_vo_private, fcc_vo_private])
        .reset_index(drop=True)
        .assign(
            Benchmark=lambda gdf: gdf["Property Use"].map(uses_linked_to_benchmarks),
        )  # link uses to benchmarks so can merge on common benchmarks
        .merge(benchmarks, how="left")
        .assign(
            bounded_area_m2=lambda df: np.where(
                (df["Area (m2)"] > 5) & (df["Area (m2)"] < df["Area Upper Bound [m²]"]),
                df["Area (m2)"],
                np.nan,
            ),  # Remove all areas outside of 5 <= area <= Upper Bound
            to_gia=lambda df: df["Basis for Area Calculation"]
            .replace(
                {
                    "GIA": 1,
                    "GEA": 0.95,
                    "NIA": 1.25,
                }
            )
            .astype("float16"),
            area_conversion_factors=lambda df: df["to_gia"]
            * df["GIA to Sales"].fillna(1),
        )
        .pipe(gpd.sjoin, small_area_boundaries, op="within")
        .rename(
            columns={
                "Typical fossil fuel [kWh/m²y]": "typical_ff",
                "Industrial space heat [kWh/m²y]": "industrial_sh",
            }
        )
        .assign(
            latitude=lambda gdf: gdf.to_crs(epsg=4326).geometry.y,
            longitude=lambda gdf: gdf.to_crs(epsg=4326).geometry.x,
            inferred_area_m2=lambda df: np.round(
                df["bounded_area_m2"].fillna(df["Typical Area [m²]"])
                * df["area_conversion_factors"]
            ),
            area_is_estimated=lambda df: df["bounded_area_m2"].isnull(),
            heating_mwh_per_year=lambda df: np.round(
                (
                    df["typical_ff"].fillna(0) * df["inferred_area_m2"]
                    + df["industrial_sh"].fillna(0) * df["inferred_area_m2"]
                )
                * 10 ** -3
            ),
            category_area_band_m2=lambda gdf: gdf.groupby("Benchmark")[
                "inferred_area_m2"
            ].transform(lambda x: str(x.min()) + " - " + str(x.max())),
            ID=lambda gdf: gdf["ID"].astype("int32"),
        )
        .loc[:, use_columns]
    )
    valuation_office_private.to_file(
        data_dir / "valuation_office_private.gpkg", driver="GPKG"
    )


def anonymise_valuation_office_private(data_dir, vo_private, vo_public):
    private_columns = ["ID", "Benchmark", "inferred_area_m2"]
    public_columns = ["ID", "benchmark_1", "Area"]
    vo_private_vs_public = vo_private[private_columns].merge(vo_public[public_columns])

    anonymised_buildings = (
        vo_private_vs_public.query("inferred_area_m2.notnull() & Area.isnull()")
        .loc[:, "ID"]
        .to_numpy()
    )

    vo_private_anonymised = vo_private.copy()
    mask = vo_private_anonymised["ID"].isin(anonymised_buildings)
    to_anonymise = (
        vo_private_anonymised.loc[mask]
        .copy()
        .assign(
            inferred_area_m2=lambda df: df["Typical Area [m²]"]
            * df["area_conversion_factors"],
            area_is_estimated=True,
            heating_mwh_per_year=lambda df: np.round(
                (
                    df["typical_ff"].fillna(0) * df["inferred_area_m2"]
                    + df["industrial_sh"].fillna(0) * df["inferred_area_m2"]
                )
                * 10 ** -3
            ),
        )
    )

    vo_private_anonymised.loc[mask] = to_anonymise
    vo_private_anonymised.to_file(
        data_dir / "valuation_office_private_anonymised.gpkg", driver="GPKG"
    )


def _clean_string(s):
    return (
        s.astype(str)
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .str.lower()
    )


def _flatten_column_names(df):
    new_names = [str(c[0]) + "_" + str(c[1]) for c in df.columns.to_flat_index()]
    df.columns = new_names
    return df


def create_m_and_r(data_dir):

    mprn = (
        pd.read_excel(data_dir / "FOI_Codema_24.1.20.xlsx", sheet_name="MPRN_data")
        .assign(
            postcode=lambda df: _clean_string(
                df["County"].replace({"Dublin (County)": "Co. Dublin"})
            ).str.title(),
            location=lambda df: _clean_string(
                df["Location"].str.replace("(,? ?Dublin \d+)", "", regex=True)
            ),  # remove postcodes as accounted for by 'postcode' column
            pb_name=lambda df: _clean_string(df["PB Name"]),
            address=lambda df: (
                df["pb_name"]
                + " "
                + df["location"]
                + " "
                + df["postcode"]
                + " "
                + "ireland"
            ).apply(lambda x: expand_address(x)[0]),
            category=lambda df: df["Consumption Category"],
        )
        .rename(
            columns={"Attributable Total Final Consumption (kWh)": "electricity_kwh"}
        )
        .loc[
            :, ["pb_name", "address", "category", "postcode", "Year", "electricity_kwh"]
        ]
    )

    gprn = (
        pd.read_excel(data_dir / "FOI_Codema_24.1.20.xlsx", sheet_name="GPRN_data")
        .assign(
            postcode=lambda df: _clean_string(
                df["County"].replace({"Dublin (County)": "Co. Dublin"})
            ).str.title(),
            location=lambda df: _clean_string(
                df["Location"].str.replace("(,? ?Dublin \d+)", "", regex=True)
            ),  # remove postcodes as accounted for by 'postcode' column
            pb_name=lambda df: _clean_string(df["PB Name"]),
            address=lambda df: (
                df["pb_name"]
                + " "
                + df["location"]
                + " "
                + df["postcode"]
                + " "
                + "ireland"
            ).apply(lambda x: expand_address(x)[0]),
            category=lambda df: df["Consumption Category"],
        )
        .rename(columns={"Attributable Total Final Consumption (kWh)": "gas_kwh"})
        .loc[:, ["pb_name", "address", "category", "postcode", "Year", "gas_kwh"]]
    )

    m_and_r = (
        mprn.merge(gprn, how="outer", indicator=True)
        .assign(
            dataset=lambda df: df["_merge"].replace(
                {"left_only": "mprn_only", "right_only": "gprn_only"}
            )
        )
        .drop(columns="_merge")
    )

    columns_to_drop = ["Year", "electricity_kwh", "gas_kwh"]
    m_and_r_by_year = (
        m_and_r.pivot_table(
            index="address", columns="Year", values=["electricity_kwh", "gas_kwh"]
        )
        .pipe(_flatten_column_names)
        .reset_index()
        .merge(m_and_r.drop(columns=columns_to_drop).drop_duplicates())
        .pipe(gpd.GeoDataFrame)
    )

    m_and_r_by_year.to_csv(data_dir / "m_and_r.csv", index=False)


def create_geocoded_m_and_r(
    data_dir, m_and_r, dublin_boundary, dublin_routing_key_boundaries
):

    m_and_r_addresses = pd.Series(m_and_r["address"].unique())

    try:
        GOOGLE_GEOCODING_API = getenv("GOOGLE_GEOCODING_API")
    except:
        logger.error(
            "Need to create a .env file and store your API key for Google Maps"
            " Geocoding API as GOOGLE_GEOCODING_API=<YOUR API KEY>"
        )

    geocoded_m_and_r_addresses_raw = gpd.tools.geocode(
        m_and_r_addresses,
        provider="google",
        api_key=GOOGLE_GEOCODING_API,
    )
    geocoded_m_and_r_addresses_raw.to_file(
        data_dir / "M&R_raw_addresses_geocoded_by_google_maps.geojson",
        driver="GeoJSON",
    )
    geocoded_m_and_r_addresses_raw = gpd.read_file(
        data_dir / "M&R_raw_addresses_geocoded_by_google_maps.geojson",
        driver="GeoJSON",
    )

    geocoded_m_and_r_addresses_clean = (
        gpd.sjoin(
            geocoded_m_and_r_addresses_raw,
            dublin_boundary.to_crs(epsg=4326),
            op="within",
        )
        .drop(columns="index_right")
        .query("address != 'Dublin, Ireland'")
        .rename(columns={"address": "google_maps_address"})
        .join(m_and_r_addresses.rename("raw_address"), how="right")
        .pipe(get_geometries_within, dublin_routing_key_boundaries.to_crs(epsg=4326))
        .merge(
            m_and_r[["postcode", "address"]],
            left_on="raw_address",
            right_on="address",
        )
        .drop_duplicates()
        .query("COUNTYNAME == postcode")  # filter out incorrect postcodes
        .drop(columns=["postcode", "address"])
    )
    geocoded_m_and_r_addresses_clean.to_file(
        data_dir / "M&R_clean_addresses_geocoded_by_google_maps.geojson",
        driver="GeoJSON",
    )
