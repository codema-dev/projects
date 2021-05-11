import json
from pathlib import Path

from loguru import logger
import numpy as np
import pandas as pd
from pdfminer.high_level import extract_text
from pdfminer.pdfparser import PDFSyntaxError
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

with open(data_dir / "ETS-EPA-Annual-Emissions.json", "w") as file:
    json.dump(emissions_by_pdf, file)