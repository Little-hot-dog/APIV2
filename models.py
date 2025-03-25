from sqlalchemy import Column, Integer, String, ForeignKey, JSON, DateTime, Float
from sqlalchemy.orm import relationship
from database import Base
from sqlalchemy.sql import func

from typing import List, Dict

class RawData(Base):
    __tablename__ = "raw_data" #Название таблицы

    id = Column(Integer, primary_key=True, index=True) #index=True - позволяет искать по данному параметру
    host = Column(String, index=True)
    data = Column(JSON)
    time_date = Column(DateTime(timezone=True), server_default=func.now())

class SystemInfo(Base):
    __tablename__ = "system_info"  # Название таблицы

    id = Column(Integer, primary_key=True, index=True)
    host = Column(String, index=True)
    param = Column(String, index=True)
    value = Column(String, index=True)
    time_date = Column(DateTime(timezone=True), server_default=func.now())


#new
from datetime import datetime
class CriticalPoint(Base):
    __tablename__ = "critical_points"

    param = Column(String, primary_key=True, index=True)
    min_value = Column(Float, nullable=True)
    max_value = Column(Float, nullable=True)
    exact_value = Column(Float, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)









