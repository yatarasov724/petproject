from datetime import datetime
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, Float, ForeignKey
from sqlalchemy.orm import relationship
from app.core.database import Base


class Signal(Base):
    __tablename__ = "signals"

    id = Column(Integer, primary_key=True, index=True)
    news_id = Column(Integer, ForeignKey("news_items.id"), nullable=False)
    ticker = Column(String(10), nullable=False, index=True)
    action = Column(String(10), nullable=False)  # BUY / SELL / HOLD
    confidence = Column(Integer, nullable=False)  # 0-100
    timeframe = Column(String(20))  # immediate / short / medium
    explanation = Column(Text)
    risk_factors = Column(Text)  # JSON список рисков
    is_market_hours = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    news = relationship("NewsItem")
