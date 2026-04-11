from datetime import datetime
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean
from app.core.database import Base


class NewsItem(Base):
    __tablename__ = "news_items"

    id = Column(Integer, primary_key=True, index=True)
    source = Column(String(50), nullable=False)
    title = Column(Text, nullable=False)
    content = Column(Text)
    url = Column(Text)
    published_at = Column(DateTime, nullable=False, index=True)
    raw_hash = Column(String(32), unique=True, index=True)  # MD5 для дедупликации
    created_at = Column(DateTime, default=datetime.utcnow)
