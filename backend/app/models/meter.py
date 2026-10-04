from sqlalchemy import Column, Date, DateTime, String
from sqlalchemy.sql import func

from backend.app.database import Base


class Meter(Base):
    __tablename__ = "meters"

    meter_id = Column(String(50), primary_key=True)
    zone = Column(String(100))
    consumer_type = Column(String(50))
    installation_date = Column(Date)
    status = Column(String(20), default="active")
    created_at = Column(
        DateTime,
        server_default=func.now()
    )