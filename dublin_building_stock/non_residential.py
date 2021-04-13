from collections import defaultdict

import geopandas as gpd
import numpy as np
import pandas as pd

from dublin_building_stock.spatial_operations import (
    get_geometries_within,
    convert_to_geodataframe,
)


def load_small_area_boundaries(data_dir):

    use_columns = ["SMALL_AREA", "EDNAME", "geometry"]
    ireland_small_area_boundaries_2011 = gpd.read_file(
        data_dir / "Census2011_Small_Areas_generalised20m"
    )[use_columns]
    dublin_boundary = gpd.read_file(
        data_dir / "dublin_boundary.geojson", driver="GeoJSON"
    )

    return get_geometries_within(
        ireland_small_area_boundaries_2011.to_crs(epsg=2157),
        dublin_boundary.to_crs(epsg=2157),
    )


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


def load_vo_public(data_dir, uses_linked_to_benchmarks, benchmarks):
    vo_data_dir = data_dir / "valuation_office"
    vo_public = pd.concat(
        [pd.read_csv(filepath) for filepath in vo_data_dir.glob("*.csv")]
    ).reset_index(drop=True)
    benchmark_columns = ["Benchmark"]
    return (
        vo_public.pipe(convert_to_geodataframe, x=" X ITM", y=" Y ITM", crs="EPSG:2157")
        .join(
            vo_public["Uses"].str.split(", ", expand=True)
        )  # Split 'USE, -' into 'USE', '-'
        .drop(columns=[2, 3])  # both are empty columns
        .rename(columns={0: "use_1", 1: "use_2", "Property Number": "ID"})
        .assign(
            # Benchmark=lambda gdf: gdf["use_1"].map(uses_linked_to_benchmarks),
            benchmark_1=lambda gdf: gdf["use_1"].map(uses_linked_to_benchmarks),
            benchmark_2=lambda gdf: gdf["use_2"].map(uses_linked_to_benchmarks),
            ID=lambda df: df["ID"].astype("int32"),
        )  # link uses to benchmarks so can merge on common benchmarks
        # .merge(vo_benchmarks, how="left")
        .merge(
            benchmarks[benchmark_columns],
            how="left",
            left_on="benchmark_1",
            right_on="Benchmark",
        )
        .merge(
            benchmarks[benchmark_columns],
            how="left",
            left_on="benchmark_2",
            right_on="Benchmark",
            suffixes=["_1", "_2"],
        )
    )


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


def load_vo_private(
    data_dir, small_area_boundaries, uses_linked_to_benchmarks, benchmarks
):
    dcc_vo_private = _load_dcc_vo_private(data_dir)
    dlrcc_vo_private = _load_dlrcc_vo_private(data_dir)
    sdcc_vo_private = _load_sdcc_vo_private(data_dir)
    fcc_vo_private = _load_fcc_vo_private(data_dir)

    return (
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
        .loc[
            :,
            [
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
                "geometry",
            ],
        ]
    )


def anonymise_vo_private(vo_private, vo_public):
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
    return vo_private_anonymised
