from sqlmodel import Field, SQLModel

class Ticker(SQLModel, table=True):
    symbol: str = Field(primary_key=True)
    return_ytd: float | None
    return_1y: float | None
    return_3y: float | None
    return_5y: float | None
    return_10y: float | None
    return_15y: float | None
    inception: float | None
    morningstar_rating: int | None
    processing_complete: int | None # Contains seconds since epoch if processing is complete
    processing_error: str | None # Contains string explaining why processing failed if processing failed
    processing_attempts: int = 0