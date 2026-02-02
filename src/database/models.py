from unicodedata import category
from sqlmodel import Field, SQLModel

"""
TT Screen Criteria:
Morningstar Rating: 4-5 stars
return_10y > 0
return_5y > 0
return_3y > 0
return_1y > 0
12b-1 fee <= 0 or null

TT Return Values:
Ticker
Name
Category
YTD Return
1 Month Return
1 Year Return
3 Year Return
5 Year Return
10 Year Return
Yield (TTM)
# of Negative Years
"""

class Ticker(SQLModel, table=True):
    # TT SCREENABLE
    symbol: str = Field(primary_key=True) # TT/FF
    name: str | None # TT
    category: str | None # TT
    return_ytd: float | None # TT
    return_1m: float | None # TT
    return_1y: float | None # TT/FF
    return_3y: float | None # TT/FF
    return_5y: float | None # TT/FF
    return_10y: float | None #TT/FF
    return_15y: float | None # FF
    yield_ttm: float | None # TT
    inception: float | None # TT/FF
    twelve_b_one_fee: float | None # TT
    morningstar_rating: int | None # TT/FF

    # Only on Page
    negative_years: int | None # TT
    risk_score: int | None # TT
    # General Metadata
    processing_complete: int | None # Contains seconds since epoch if processing is complete
    processing_error: str | None # Contains string explaining why processing failed if processing failed
    processing_attempts: int = 0
    # TT Metadata
    filter_failures: str | None # Contains a string explaining why symbol does not qualify if it does not qualify

    url: str | None
