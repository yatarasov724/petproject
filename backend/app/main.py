from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.core.database import Base, engine
from app.api.routes import signals, ws
from app.scheduler.tasks import start_scheduler
from app.ai.groq_client import _worker
import asyncio

Base.metadata.create_all(bind=engine)

app = FastAPI(title="MOEX News Assistant")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.frontend_url],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(signals.router)
app.include_router(ws.router)


@app.on_event("startup")
async def startup():
    asyncio.create_task(_worker())
    start_scheduler()
