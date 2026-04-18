FROM python:3.11-slim

# System deps: ca-certificates for HTTPS RSS feeds
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (separate layer — only rebuilds on requirements change)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY app/ ./app/

# SQLite data lives outside the image so it survives container rebuilds.
# Mount a host directory or named volume to /data at runtime.
RUN mkdir -p /data

# Structured JSON logs by default in Docker (parse with any log aggregator)
ENV LOG_FORMAT=json
ENV LOG_LEVEL=INFO
ENV DATABASE_URL=sqlite:////data/moex_assistant.db

EXPOSE 8000

# Single worker: multiple workers don't share in-memory metrics and SQLite
# WAL handles concurrent reads fine with a single writer process.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
