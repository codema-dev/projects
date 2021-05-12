import json
from os import getenv
from pathlib import Path
import re

import geopandas as gpd
import pandas as pd
from pdfminer.high_level import extract_text
from pdfminer.pdfparser import PDFSyntaxError
import requests
from tqdm import tqdm

from dublin_building_stock.string_operations import extract_annual_ets_emissions
from dublin_building_stock.string_operations import extract_ets_address

data_dir = Path("data")
download_dir = data_dir / "EPA-ETS"
filepaths = list(download_dir.glob("*.pdf"))

data_by_pdf = {}
invalid_pdfs = []
for filepath in tqdm(filepaths):
    try:
        raw_text = extract_text(filepath)
    except PDFSyntaxError:
        invalid_pdfs.append(filepath.stem)
    else:
        address = extract_ets_address(raw_text)
        annual_emissions = extract_annual_ets_emissions(raw_text)
        data_by_pdf[filepath.stem] = {"Address": address, "Emissions": annual_emissions}

with open(data_dir / "epa_ets_data.json", "w") as file:
    json.dump(data_by_pdf, file)

pdf_ids = [re.findall("(GHG\d+)", id)[0] for id in data_by_pdf]
emissions_by_pdf_ids = {
    id: v["Emissions"] for id, (k, v) in zip(pdf_ids, data_by_pdf.items())
}
address_by_pdf_ids = {
    id: v["Address"] for id, (k, v) in zip(pdf_ids, data_by_pdf.items())
}

response = requests.get(
    "http://www.epa.ie/climate/emissionstradingoverview/etscheme/accesstocurrentpermits/#d.en.64017"
)
# https://www.seai.ie/data-and-insights/seai-statistics/conversion-factors/
electricity_2019_tco2_per_tj = 90.1
tj_to_mwh = 1 / 0.0036
epa_ets_listings = (
    pd.read_html(response.content)[0]
    .rename(columns={"Operator Name": "Name", "Reg No": "License"})
    .assign(
        ID=lambda df: df["License"].str.extract(r"(GHG\d+)")[0],
        Address=lambda df: df["ID"].map(address_by_pdf_ids),
        metered_annual_emissions_tco2=lambda df: df["ID"]
        .map(emissions_by_pdf_ids)
        .astype("float32"),
        estimated_annual_electricity_mwh=lambda df: df["metered_annual_emissions_tco2"]
        * electricity_2019_tco2_per_tj
        * tj_to_mwh,
    )
)
epa_ets_listings.to_excel(data_dir / "epa_ets_sites.xlsx")

epa_ets_listings_dublin = epa_ets_listings[epa_ets_listings["Address"].str.contains("Dublin")].query("Name != 'Aurivo Dairy Ingredients Ltd'")

# Manually match EPA ETS sites to Valuation Office Locations
with open(data_dir / "data/epa_ets_site_data.json.json", "r") as file:
    epa_ets_site_manual_data = json.load(file)

epa_ets_site_valuation_office_id = {k: v["ID"] for k,v in epa_ets_site_manual_data.items()}
epa_ets_site_uses = {k: v["Use"] for k,v in epa_ets_site_manual_data.items()}
epa_ets_site_latitude = {k: v["Latitude"] for k,v in epa_ets_site_manual_data.items()}
epa_ets_site_longitude = {k: v["Longitude"] for k,v in epa_ets_site_manual_data.items()}

epa_ets_listings_dublin_filled = epa_ets_listings_dublin.assign(
    ID=lambda df: df["License"].map(epa_ets_site_valuation_office_id),
    Use=lambda df: df["License"].map(epa_ets_site_uses),
    Latitude=lambda df: df["License"].map(epa_ets_site_latitude),
    Longitude=lambda df: df["License"].map(epa_ets_site_longitude),
)

epa_ets_listings_dublin_filled.to_excel(
    data_dir / "epa_ets_sites_dublin.xlsx"
)
