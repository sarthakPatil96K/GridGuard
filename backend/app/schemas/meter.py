from datetime import date, datetime

from pydantic import BaseModel


class MeterResponse(BaseModel):
    meter_id: str
    zone: str | None = None
    consumer_type: str | None = None
    installation_date: date | None = None
    status: str | None = None
    created_at: datetime | None = None

    class Config:
        from_attributes = True