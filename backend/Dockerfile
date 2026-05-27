FROM python:3.11-slim

# System deps: ca-certificates for HTTPS RSS feeds
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (separate layer — only rebuilds on requirements change)
COPY requirements.txt .
RUN pip install --no-cache-dir --timeout 120 --retries 5 -r requirements.txt

# Pre-cache the embedding model so deploys do not re-download 120 MB at runtime
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')"

# Copy application source
COPY app/ ./app/

# Structured JSON logs by default in Docker (parse with any log aggregator)
ENV LOG_FORMAT=json
ENV LOG_LEVEL=INFO

EXPOSE 8000

# Dev/test dependencies and test sources (for CI and TDD workflows)
COPY requirements-dev.txt .
RUN pip install --no-cache-dir --timeout 120 --retries 5 -r requirements-dev.txt

COPY tests/ ./tests/

# PORT is injected by Railway (and most other PaaS). Falls back to 8000 locally.
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000} --workers 1"]
