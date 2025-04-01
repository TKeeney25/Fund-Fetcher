from unicodedata import category
from sqlmodel import Field, SQLModel



class Ticker(SQLModel, table=True):
    # TT SCREENABLE
    symbol: str = Field(primary_key=True) # TT/FF
    return_ytd: float | None # TT
    return_1m: float | None # TT
    return_1y: float | None # TT/FF
    return_3y: float | None # TT/FF
    return_5y: float | None # TT/FF
    return_10y: float | None #TT/FF
    return_15y: float | None # FF
    inception: float | None # TT/FF
    category: str | None # TT
    twelve_b_one_fee: float | None # TT
    morningstar_rating: int | None # TT/FF

    # Only on Page
    negative_years: int | None # TT
    risk_score: int | None # TT
    # Metadata
    processing_complete: int | None # Contains seconds since epoch if processing is complete
    processing_error: str | None # Contains string explaining why processing failed if processing failed
    processing_attempts: int = 0
