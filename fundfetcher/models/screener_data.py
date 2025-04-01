from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

class ScreenerData(BaseModel):
    symbol: str | None = Field(None, alias="ticker")
    name: str | None = None
    category: str | None = Field(None, alias="morningstar category")
    return_ytd: float | None = Field(None, alias="total return ytd")
    return_1m: float | None = Field(None, alias="total return 1 month")
    return_1y: float | None = Field(None, alias="total return 1 year")
    return_3y: float | None = Field(None, alias="total return 3 year")
    return_5y: float | None = Field(None, alias="total return 5 year")
    return_10y: float | None = Field(None, alias="total return 10 year")
    ttm_yield: float | None = Field(None, alias="ttm yield")
    morningstar_rating: int | None = Field(None, alias="morningstar rating for funds overall")
    twelve_b_one_fee: float | None = Field(None, alias="12b-1 fee")

def etl(title_data:List[str], data:List[Any]) -> ScreenerData:
    data_dict:Dict[str, Any] = {}
    for i, title in enumerate(title_data):
        data_dict[title.lower()] = data[i]
    return ScreenerData(**data_dict)
