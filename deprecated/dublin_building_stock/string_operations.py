import re
from typing import List

COUNTIES = [
    "Antrim",
    "Armagh",
    "Carlow",
    "Cavan",
    "Clare",
    "Cork",
    "Donegal",
    "Down",
    "Dublin",
    "Fermanagh",
    "Galway",
    "Kerry",
    "Kildare",
    "Kilkenny",
    "Laois",
    "Leitrim",
    "Limerick",
    "Londonderry",
    "Longford",
    "Louth",
    "Mayo",
    "Meath",
    "Monaghan",
    "Offaly",
    "Roscommon",
    "Sligo",
    "Tipperary",
    "Tyrone",
    "Waterford",
    "Westmeath",
    "Wexford",
    "Wicklow",
]

SITES = ["Drogheda"]


def extract_ets_address(raw_text: str) -> str:
    header_page_raw = re.findall(
        r"(Permit Register Number:.+?)(?=\x0c)", raw_text[:2000], flags=re.DOTALL
    )[0]
    header_page_split = re.split(r"\n\n", header_page_raw)
    locations = SITES + COUNTIES
    address_raw = [
        s for s in header_page_split if any([l.lower() in s.lower() for l in locations])
    ]
    assert len(address_raw) > 1, "Failed to extract both Operator & Location"

    # Remove Organisation names such as 'Dublin Power Plant' that aren't either the
    # Operator or Location
    address = [
        a for a in address_raw if a in sorted(address_raw, key=len, reverse=True)[:2]
    ]
    return address[-1]


def extract_annual_ets_emissions(raw_text: str) -> float:
    annual_emissions = re.findall(
        r"Estimated Annual Emissions \(tonnes CO2\(e\)\)\n\n(\d+)", raw_text
    )[0]
    return float(annual_emissions)