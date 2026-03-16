"""
Postcode to UK region mapper.
Uses the postcode prefix (outward code area) to derive a consistent
UK region. This is more reliable than the 'country'/'region' fields
returned by Companies House which are inconsistent.
"""

# Mapping of postcode area prefix → UK region
# Based on Royal Mail postcode areas
POSTCODE_AREA_TO_REGION = {
    # Greater London
    "EC": "Greater London", "WC": "Greater London",
    "E": "Greater London",  "N": "Greater London",
    "NW": "Greater London", "SE": "Greater London",
    "SW": "Greater London", "W": "Greater London",
    "W1": "Greater London", "WC2": "Greater London",
    "BR": "Greater London", "CR": "Greater London",
    "DA": "Greater London", "EN": "Greater London",
    "HA": "Greater London", "IG": "Greater London",
    "KT": "Greater London", "RM": "Greater London",
    "SM": "Greater London", "TW": "Greater London",
    "UB": "Greater London", "WD": "Greater London",

    # South East England
    "BN": "South East England", "CT": "South East England",
    "GU": "South East England", "HP": "South East England",
    "ME": "South East England", "MK": "South East England",
    "OX": "South East England", "PO": "South East England",
    "RG": "South East England", "RH": "South East England",
    "SL": "South East England", "SO": "South East England",
    "SP": "South East England", "TN": "South East England",

    # South West England
    "BA": "South West England", "BH": "South West England",
    "BS": "South West England", "DT": "South West England",
    "EX": "South West England", "GL": "South West England",
    "PL": "South West England", "SN": "South West England",
    "TA": "South West England", "TQ": "South West England",
    "TR": "South West England",

    # East of England
    "AL": "East of England", "CB": "East of England",
    "CM": "East of England", "CO": "East of England",
    "IP": "East of England", "LU": "East of England",
    "NR": "East of England", "PE": "East of England",
    "SG": "East of England", "SS": "East of England",

    # East Midlands
    "DE": "East Midlands", "DN": "East Midlands",
    "LE": "East Midlands", "LN": "East Midlands",
    "NG": "East Midlands", "NN": "East Midlands",

    # West Midlands
    "B": "West Midlands",  "CV": "West Midlands",
    "DY": "West Midlands", "HR": "West Midlands",
    "ST": "West Midlands", "TF": "West Midlands",
    "WR": "West Midlands", "WS": "West Midlands",
    "WV": "West Midlands",

    # North West England
    "BB": "North West England", "BL": "North West England",
    "CA": "North West England", "CH": "North West England",
    "CW": "North West England", "FY": "North West England",
    "L": "North West England",  "LA": "North West England",
    "M": "North West England",  "OL": "North West England",
    "PR": "North West England", "SK": "North West England",
    "WA": "North West England", "WN": "North West England",

    # North East England
    "DH": "North East England", "DL": "North East England",
    "NE": "North East England", "SR": "North East England",
    "TS": "North East England",

    # Yorkshire and the Humber
    "BD": "Yorkshire", "DN": "Yorkshire",
    "HD": "Yorkshire", "HG": "Yorkshire",
    "HU": "Yorkshire", "HX": "Yorkshire",
    "LS": "Yorkshire", "S": "Yorkshire",
    "WF": "Yorkshire", "YO": "Yorkshire",

    # Scotland
    "AB": "Scotland", "DD": "Scotland",
    "DG": "Scotland", "EH": "Scotland",
    "FK": "Scotland", "G": "Scotland",
    "HS": "Scotland", "IV": "Scotland",
    "KA": "Scotland", "KW": "Scotland",
    "KY": "Scotland", "ML": "Scotland",
    "PA": "Scotland", "PH": "Scotland",
    "TD": "Scotland", "ZE": "Scotland",

    # Wales
    "CF": "Wales", "LD": "Wales",
    "LL": "Wales", "NP": "Wales",
    "SA": "Wales", "SY": "Wales",

    # Northern Ireland
    "BT": "Northern Ireland",
}


def postcode_to_region(postcode: str) -> str | None:
    """
    Derive a UK region from a postcode.
    Returns None if the postcode is unrecognised.
    """
    if not postcode:
        return None

    # Normalise — uppercase, strip spaces
    pc = postcode.upper().strip().replace(" ", "")

    if not pc:
        return None

    # Extract the area code (letters at the start)
    area = ""
    for ch in pc:
        if ch.isalpha():
            area += ch
        else:
            break

    if not area:
        return None

    # Try longest match first (e.g. "SW" before "S")
    for length in [3, 2, 1]:
        candidate = area[:length]
        if candidate in POSTCODE_AREA_TO_REGION:
            return POSTCODE_AREA_TO_REGION[candidate]

    return None


def extract_region_from_address(address: dict) -> str | None:
    """
    Extract a normalised UK region from a Companies House address dict.
    Tries postcode first (most reliable), then falls back to
    the region/county/country fields.
    """
    if not address:
        return None

    # Primary: derive from postcode
    postcode = address.get("postal_code") or address.get("postcode") or ""
    region = postcode_to_region(postcode)
    if region:
        return region

    # Fallback: use CH region/county/country fields
    raw = (
        address.get("region")
        or address.get("county")
        or address.get("country")
        or ""
    ).strip()

    if not raw:
        return None

    # Normalise common CH values
    normalised = _normalise_ch_region(raw)
    return normalised


def _normalise_ch_region(raw: str) -> str:
    """Normalise inconsistent Companies House region strings."""
    mapping = {
        "united kingdom": "United Kingdom",
        "england": "England",
        "wales": "Wales",
        "scotland": "Scotland",
        "northern ireland": "Northern Ireland",
        "great britain": "United Kingdom",
        "uk": "United Kingdom",
        "gb": "United Kingdom",
    }
    return mapping.get(raw.lower(), raw.title())
