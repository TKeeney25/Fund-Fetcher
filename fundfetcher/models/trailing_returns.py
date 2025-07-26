from typing import Dict, List, Optional
from pydantic import BaseModel, Field

class TrailingReturns(BaseModel):
    one_day: Optional[float] = Field(None, alias="1-day")
    one_week: Optional[float] = Field(None, alias="1-week")
    one_month: Optional[float] = Field(None, alias="1-month")
    three_month: Optional[float] = Field(None, alias="3-month")
    one_year: Optional[float] = Field(None, alias="1-year")
    three_year: Optional[float] = Field(None, alias="3-year")
    five_year: Optional[float] = Field(None, alias="5-year")
    ten_year: Optional[float] = Field(None, alias="10-year")
    fifteen_year: Optional[float] = Field(None, alias="15-year")
    ytd: Optional[float] = Field(None, alias="ytd")
    inception: Optional[float] = Field(None, alias="earliest available")

def etl(title_data:List[str], raw_data:List[str]) -> TrailingReturns:
    data_dict:Dict[str, Optional[float]] = {}
    for i, title in enumerate(title_data):
        try:
            data_dict[title.lower().strip()] = float(raw_data[i])
        except ValueError:
            data_dict[title.lower()] = None
    return TrailingReturns(**data_dict)
