from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
# from sqlalchemy import Column, Integer, String, ForeignKey, JSON, DateTime


class RawDataRequest(BaseModel):
    data: List[dict]

class SystemInfoResponse(BaseModel):
    id: int
    host: str
    param: str
    value: str
    time_date: datetime


#new code
class CriticalPointCreate(BaseModel):
    param: str
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    exact_value: Optional[float] = None

class CriticalPointResponse(BaseModel):
    param: str
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    exact_value: Optional[float] = None
    created_at: datetime

    class Config:
        orm_mode = True
