from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.models.signal import Signal
from app.models.news import NewsItem

router = APIRouter(prefix="/api")


@router.get("/signals")
def get_signals(
    limit: int = Query(50, le=200),
    ticker: str | None = None,
    action: str | None = None,
    db: Session = Depends(get_db),
):
    q = db.query(Signal).order_by(Signal.created_at.desc())
    if ticker:
        q = q.filter(Signal.ticker == ticker.upper())
    if action:
        q = q.filter(Signal.action == action.upper())
    return q.limit(limit).all()


@router.get("/signals/{signal_id}")
def get_signal(signal_id: int, db: Session = Depends(get_db)):
    return db.query(Signal).filter(Signal.id == signal_id).first()


@router.get("/news")
def get_news(limit: int = Query(100, le=500), db: Session = Depends(get_db)):
    return db.query(NewsItem).order_by(NewsItem.published_at.desc()).limit(limit).all()


@router.get("/health")
def health():
    return {"status": "ok"}


@router.get("/market/status")
def market_status():
    from app.scheduler.tasks import is_market_hours
    return {"is_open": is_market_hours()}
