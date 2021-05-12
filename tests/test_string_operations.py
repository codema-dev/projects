import pytest

from dublin_building_stock import string_operations


@pytest.mark.parametrize(
    "raw_text,expected_output",
    [
        (
            "Headquarters, \nJohnstown Castle Estate,\nCounty Wexford, Ireland\n\nGREENHOUSE GAS EMISSIONS PERMIT\n\nPermit Register Number:\n\nIE-GHG182-10506-1\n\nOperator:\n\nAlexion Pharma International \nOperations Unlimited Company\nCollege Business & Technology Park,\nBlanchardstown Road North\nDublin 15\nD15 R925\n\nAlexion College Park\n\nCollege Business & Technology Park\nBlanchardstown Road North\nDublin 15\nIreland\n\nInstallation Name:\n\nAlexion College Park\n\nSite Name:\n\nLocation:\n\n\x0c\x0c",
            "College Business & Technology Park\nBlanchardstown Road North\nDublin 15\nIreland",
        ),
        (
            "Headquarters, \nJohnstown Castle Estate,\nCounty Wexford, Ireland\n\nGREENHOUSE GAS EMISSIONS PERMIT\n\nPermit Register Number:\n\nIE-GHG172-10469-4\n\nAmazon Data Services Ireland Limited\nOne Burlington Plaza\nBurlington Road\nDublin 4\nD04RH96\n\nOperator:\n\nSite Name:\n\nLocation:\n\nInstallation Name:\n\nDUB9\n\nADSIL DUB9\n\nGreenhills Road\nTallaght\nDublin 24\nIreland\n\n\x0c",
            "Greenhills Road\nTallaght\nDublin 24\nIreland",
        ),
        (
            "Headquarters, \nJohnstown Castle Estate,\nCounty Wexford, Ireland\n\nGREENHOUSE GAS EMISSIONS PERMIT\n\nPermit Register Number:\n\nIE-GHG091-10393-2\n\nOperator:\n\nTynagh Energy Limited\nThe Crescent Building\nNorthwood Park\nSantry\nDublin\nD09 X8W3\n\nTynagh 400MW CCPP\n\nDerryfrench\nTynagh\nLoughrea\nGalway\n\nInstallation Name:\n\nTynagh 400MW CCPP\n\nSite Name:\n\nLocation:\n\n\x0c",
            "Derryfrench\nTynagh\nLoughrea\nGalway",
        ),
        (
            "Headquarters, \nJohnstown Castle Estate,\nCounty Wexford, Ireland\n\nGREENHOUSE GAS EMISSIONS PERMIT\n\nPermit Register Number:\n\nIE-GHG200-10527-1\n\nOperator:\n\nAmazon Data Services Ireland Limited\nOne Burlington Plaza\nBurlington Road\nDublin 4\nDublin\nD04RH96\n\nInstallation Name:\n\nData Processing Centre - DUB 62\n\nSite Name:\n\nLocation:\n\nData Processing Centre - DUB 62\n\nDrogheda IDA Business and Technology \nPark\nDonore Road\nDrogheda\n\n\x0cMeath\nIreland\n\n\x0c",
            "Drogheda IDA Business and Technology \nPark\nDonore Road\nDrogheda",
        ),
        (
            "Headquarters, \nJohnstown Castle Estate,\nCounty Wexford, Ireland\n\nGREENHOUSE GAS EMISSIONS PERMIT\n\nPermit Register Number:\n\nIE-GHG016-10346-4\n\nOperator:\n\nSynergen Power Limited\nESB Dublin Bay\nPigeon House Road\nRingsend\nDublin\nD04 Y5N2\n\nDublin Bay Power Plant\n\nPigeon House Road\nRingsend\nDublin 4\nIreland\n\nInstallation Name:\n\nDublin Bay Power Plant\n\nSite Name:\n\nLocation:\n\n\x0c",
            "Pigeon House Road\nRingsend\nDublin 4\nIreland",
        ),
        (
            "Headquarters, \nJohnstown Castle Estate,\nCounty Wexford, Ireland\n\nGREENHOUSE GAS EMISSIONS PERMIT\n\nPermit Register Number:\n\nIE-GHG190-10516-1\n\nOperator:\n\nDigital Netherlands VIII B.V.\nStratus House, Unit 1\n1st  Floor, College Business & \nTechnology Park\nBlanchardstown\nDUBLIN 15\n\nInstallation Name:\n\nDUB14 Profile Park\n\nSite Name:\n\nLocation:\n\nDUB14 Profile Park\n\nGrange Castle, Nangor Road\nDublin 22\nIreland\n\n\x0c",
            "Grange Castle, Nangor Road\nDublin 22\nIreland",
        ),
    ],
)
def test_extract_ets_address(raw_text, expected_output) -> None:
    output = string_operations.extract_ets_address(raw_text)
    assert output == expected_output


def test_extract_annual_ets_emissions() -> None:
    raw_text = "Estimated Annual Emissions (tonnes CO2(e))\n\n1450000"
    expected_output = float(1450000)
    output = string_operations.extract_annual_ets_emissions(raw_text)
    assert output == expected_output
