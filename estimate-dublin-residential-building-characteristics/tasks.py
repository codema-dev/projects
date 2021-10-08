from csv import QUOTE_NONE
import json
from os import PathLike
from typing import Any
from zipfile import ZipFile

import pandas as pd
import requests


def download_building_energy_ratings(product: PathLike) -> None:
    cookies = {
        "ASP.NET_SessionId": "gvb1njrssax1kcmjyzatuf3x",
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://ndber.seai.ie",
        "Connection": "keep-alive",
        "Referer": "https://ndber.seai.ie/BERResearchTool/ber/search.aspx",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "DNT": "1",
        "Sec-GPC": "1",
    }

    data = {
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__VIEWSTATE": "/wEPDwULLTE2MDEwODU4NjAPFgIeE1ZhbGlkYXRlUmVxdWVzdE1vZGUCARYCZg9kFgICAw9kFgICAw8WAh4FY2xhc3MFC21haW53cmFwcGVyFgICBQ8PFgIeB1Zpc2libGVnZGRk/o+nX293q8AvTPN9mLTMfC2ZSlFasqiSYYRZIXBq1B8=",
        "__VIEWSTATEGENERATOR": "AFB8016F",
        "__SCROLLPOSITIONX": "0",
        "__SCROLLPOSITIONY": "280",
        "__EVENTVALIDATION": "/wEdAAPEuLD//Lrnct58vIDU2Hx9Xw+uRy4COswXgj8OGX6Nym8pc/9FsasLndkJePd0e319WvmW++umNulm4SeWaVFh1FOy2200t0nXvcBQEo5kHw==",
        "ctl00$DefaultContent$BERSearch$dfExcelDownlaod$DownloadAllData": "Download All Data",
    }

    response = requests.post(
        "https://ndber.seai.ie/BERResearchTool/ber/search.aspx",
        headers=headers,
        cookies=cookies,
        data=data,
    )

    with open(product, "wb") as f:
        for chunk in response.iter_content(chunk_size=4096):
            f.write(chunk)


def extract_columns_and_standardise_column_names(
    product: Any, upstream: Any, dtypes_path: PathLike, names_path: PathLike
) -> None:

    with open(dtypes_path, "r") as f:
        dtypes = json.load(f)
    with open(names_path, "r") as f:
        names = json.load(f)

    with ZipFile(upstream["download_building_energy_ratings"]) as zf:
        with zf.open("BERPublicsearch.txt", "r") as f:
            building_energy_ratings = pd.read_csv(
                f,
                sep="\t",
                usecols=dtypes.keys(),
                dtype=dtypes,
                encoding="latin-1",
                quoting=QUOTE_NONE,
            ).rename(columns=names)

    building_energy_ratings.to_parquet(product)
