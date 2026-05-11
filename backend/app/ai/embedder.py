"""
Sentence embedding for news title clustering.

Model: paraphrase-multilingual-MiniLM-L12-v2
  - ~120 MB, supports Russian, ~5 ms/title on CPU
  - Downloaded to ~/.cache/huggingface/ on first use

Thread safety: double-checked locking for init; encode() is safe for
concurrent CPU calls (GIL + numpy).
"""

import logging
import threading

import numpy as np

logger = logging.getLogger(__name__)

_MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"

_lock  = threading.Lock()
_model = None


def _get_model():
    global _model
    if _model is None:
        with _lock:
            if _model is None:
                from sentence_transformers import SentenceTransformer  # noqa: PLC0415
                logger.info("Loading embedding model %s", _MODEL_NAME)
                _model = SentenceTransformer(_MODEL_NAME)
                logger.info("Embedding model loaded")
    return _model


def embed(text: str) -> bytes:
    """Encode *text*, L2-normalise, return as float32 bytes (BLOB-friendly)."""
    vec = _get_model().encode(text, normalize_embeddings=True, show_progress_bar=False)
    return vec.astype("float32").tobytes()


def cosine(a: bytes, b: bytes) -> float:
    """Dot product of two L2-normalised float32 embeddings (= cosine similarity)."""
    va = np.frombuffer(a, dtype="float32")
    vb = np.frombuffer(b, dtype="float32")
    return float(np.dot(va, vb))
