# %%
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from dublin_building_stock.non_residential import (
    load_uses_benchmarks,
    load_benchmarks,
    create_valuation_office_public,
    create_valuation_office_private,
    anonymise_valuation_office_private,
    create_m_and_r,
    create_geocoded_m_and_r,
)

data_dir = Path("../data")


# %%
dublin_boundary = gpd.read_file(data_dir / "dublin_boundary.geojson", driver="GeoJSON")

# %%
dublin_routing_key_boundaries = gpd.read_file(
    data_dir / "dublin_routing_key_boundaries.geojson", driver="GeoJSON"
)

# %%
dublin_postcode_boundaries = (
    dublin_routing_key_boundaries.dissolve(by="COUNTYNAME")
    .drop(columns=["RoutingKey", "Descriptor"])
    .to_crs(epsg=4326)  # keep in same crs as geocoded results
)

# %%
small_area_boundaries_2011 = gpd.read_file(
    data_dir / "Dublin_Census2011_Small_Areas_generalised20m"
)

# %%
uses_linked_to_benchmarks = load_uses_benchmarks(data_dir)

# %%
benchmarks = load_benchmarks(data_dir)

# %%
create_valuation_office_public(
    data_dir, uses_linked_to_benchmarks, benchmarks, small_area_boundaries_2011
)

# %%
create_valuation_office_private(
    data_dir, uses_linked_to_benchmarks, benchmarks, small_area_boundaries_2011
)

# %%
vo_public = gpd.read_file(data_dir / "valuation_office_public.gpkg", driver="GPKG")
vo_private = gpd.read_file(data_dir / "valuation_office_private.gpkg", driver="GPKG")
anonymise_valuation_office_private(data_dir, vo_private, vo_public)

# %%
create_m_and_r(data_dir)

# %%
m_and_r = pd.read_csv(data_dir / "m_and_r.csv")
create_geocoded_m_and_r(
    data_dir,
    m_and_r,
    dublin_boundary,
    dublin_routing_key_boundaries,
    provider="nominatim",
    domain="localhost:8080",
    get_latest=True,
)

# %%
# provider="google_maps"
provider = "nominatim"
m_and_r_locations = gpd.read_file(
    data_dir / f"M&R_clean_addresses_geocoded_by_{provider}.geojson", driver="GeoJSON"
)
use_columns = [
    "address",
    "category",
    "postcode",
    "to_geocode",
    f"{provider}_address",
    "dataset",
    "latitude",
    "longitude",
] + [c for c in m_and_r.columns if any(str(x) in c for x in range(2014, 2019, 1))]
m_and_r_geocoded = (
    m_and_r_locations.merge(
        m_and_r,
        how="right",
    )
    .set_index("postcode")
    .combine_first(dublin_postcode_boundaries)
    .pipe(gpd.GeoDataFrame, crs="EPSG:4326")
    .drop_duplicates(subset="address")
    .reset_index()
    .rename(columns={"index": "postcode"})
    .assign(
        latitude=lambda gdf: gdf.geometry.representative_point().y,
        longitude=lambda gdf: gdf.geometry.representative_point().x,
    )
    .loc[:, use_columns]
)

# %%
m_and_r_geocoded.to_csv(data_dir / f"FOI_Codema_24.1.20_{provider}.csv", index=False)

# %%
m_and_r_geocoded_none_missing = (
    m_and_r_geocoded.assign(
        google_maps_geocoding_was_successful=lambda gdf: gdf[
            "google_maps_address"
        ].notnull()
    )
    .drop(columns=["google_maps_address", "latitude", "longitude"])
    .drop_duplicates()
)

m_and_r_geocoded_none_missing.to_csv(
    data_dir / "FOI_Codema_24.1.20_none_missing.csv", index=False
)

# %%
from dublin_building_stock.spatial_operations import convert_to_geodataframe

vo_public = gpd.read_file(
    data_dir / "valuation_office_public.gpkg", driver="GPKG"
).drop_duplicates(subset="ID")
epa_industrial_sites = pd.read_excel(data_dir / "epa_industrial_sites.xlsx").pipe(
    convert_to_geodataframe, y="Latitude", x="Longitude", crs="EPSG:4326"
)

# %%
industrial_benchmarks = [
    "Manufacturing (general)",
    "Engineering",
    "Laboratories",
    "Food (cooked)",
    "Food (cold)",
    "Cold storage",
    "Terminal",
    "Data Centre",
    "Paper",
]
vo_public_extract = (
    vo_public.assign(
        latitude=lambda gdf: gdf.to_crs(epsg=4326).geometry.y,
        longitude=lambda gdf: gdf.to_crs(epsg=4326).geometry.x,
    )
    .query(f"Benchmark in {industrial_benchmarks}")
    .loc[
        :,
        [
            "ID",
            "use_1",
            "Benchmark",
            " Address 1",
            "Address 2",
            "latitude",
            "longitude",
        ],
    ]
)

# %%
vo_public_extract.to_csv(data_dir / "VO_INDUSTRIAL_LOCATIONS.csv", index=False)

# %%
epa_industrial_sites_extract = epa_industrial_sites.assign(
    latitude=lambda gdf: gdf.to_crs(epsg=4326).geometry.y,
    longitude=lambda gdf: gdf.to_crs(epsg=4326).geometry.x,
).loc[:, ["Name", "Address", "Use", "latitude", "longitude"]]

# %%
epa_industrial_sites_extract.to_csv(
    data_dir / "EPA_INDUSTRIAL_LOCATIONS.csv", index=False
)
