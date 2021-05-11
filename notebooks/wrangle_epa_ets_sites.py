import json
from pathlib import Path
import re

import pandas as pd
from pdfminer.high_level import extract_text
from pdfminer.pdfparser import PDFSyntaxError
import requests
from tqdm import tqdm

data_dir = Path("../data")
download_dir = data_dir / "EPA-ETS"
filepaths = list(download_dir.glob("*.pdf"))

emissions_by_pdf = {}
invalid_pdfs = []
for filepath in tqdm(filepaths):
    try:
        raw_text = extract_text(filepath).split("\n")
    except PDFSyntaxError:
        invalid_pdfs.append(filepath.stem)
    else:
        index = [
            i
            for i, x in enumerate(raw_text)
            if "estimated annual emissions (tonnes co2(e))" in x.lower()
        ][0]
        annual_emissions = raw_text[index + 2]
        emissions_by_pdf[filepath.stem] = annual_emissions

with open(data_dir / "ets_epa_annual_emissions.json", "w") as file:
    json.dump(emissions_by_pdf, file)

pdf_ids = [re.findall("(GHG\d+)", id)[0] for id in emissions_by_pdf]
emissions_by_pdf_ids = {id: v for id, (k, v) in zip(pdf_ids, emissions_by_pdf.items())}

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
        annual_emissions_tco2=lambda df: df["ID"]
        .map(emissions_by_pdf_ids)
        .astype("float32"),
        annual_electricity_mwh=lambda df: df["annual_emissions_tco2"]
        * electricity_2019_tco2_per_tj
        * tj_to_mwh,
    )
)
epa_ets_listings.to_csv(data_dir / "epa_ets_annual_emissions.csv", index=False)