import json
from typing import Optional
from pydantic import BaseModel, Field, field_validator

class BodyContent(BaseModel):
    target: str = Field(...)
    healthcheck: Optional[bool] = Field(None)

class SQSEvent(BaseModel):
    message_id: str = Field(..., alias="MessageId")
    receipt_handle: str = Field(..., alias="ReceiptHandle")
    body: BodyContent = Field(..., alias="Body")
    attributes: dict = Field(..., alias="Attributes")
    md5_of_body: str = Field(..., alias="MD5OfBody")

    @field_validator("body", mode="before")
    def parse_body(cls, value):
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Body is not valid JSON: {exc}") from exc
        return value

