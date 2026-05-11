"""
Tests for app.pipeline.price_history and the _format_correlations formatter.

Covers:
- fetch_price: MOEX ISS response parsing (mocked aiohttp)
- capture_price_snapshot: inserts DB rows for each ticker
- update_price_snapshots: fills price_1h / price_24h for pending rows
- get_correlations: avg 24h change, min_samples filter, event_type filter
- _format_correlations: MarkdownV2 output
"""
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, AsyncMock, MagicMock

from app.db import queries
from app.pipeline.price_history import (
    fetch_price,
    capture_price_snapshot,
    update_price_snapshots,
    get_correlations,
    TickerCorrelation,
    _MIN_SAMPLES,
)
from app.telegram.formatter import _format_correlations
from tests.conftest import db  # noqa: F401


# ── helpers ───────────────────────────────────────────────────────────────────

def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _seed_cluster(db) -> int:
    return queries.create_cluster(
        db,
        canonical_title="ЦБ повысил ключевую ставку до 21 процента",
        title_tokens="21 ключевую повысил процента ставку цб",
        keywords="ставка цб",
        score=30,
    )


def _insert_snapshot(
    db,
    cluster_id: int,
    ticker: str,
    event_type: str,
    price_at_publish: float | None,
    price_24h: float | None = None,
    published_at: str | None = None,
) -> None:
    if published_at is None:
        published_at = _iso(datetime.now(timezone.utc) - timedelta(hours=30))
    db.execute(
        """
        INSERT INTO price_snapshots
            (cluster_id, ticker, event_type, published_at, price_at_publish, price_24h)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (cluster_id, ticker, event_type, published_at, price_at_publish, price_24h),
    )
    db.commit()


def _mock_aiohttp(status: int, json_data: dict):
    """Return a patch context manager that mocks aiohttp.ClientSession."""
    resp = AsyncMock()
    resp.status = status
    resp.json = AsyncMock(return_value=json_data)

    get_cm = MagicMock()
    get_cm.__aenter__ = AsyncMock(return_value=resp)
    get_cm.__aexit__ = AsyncMock(return_value=False)

    session = MagicMock()
    session.get = MagicMock(return_value=get_cm)

    session_cm = MagicMock()
    session_cm.__aenter__ = AsyncMock(return_value=session)
    session_cm.__aexit__ = AsyncMock(return_value=False)

    return patch("aiohttp.ClientSession", return_value=session_cm)


# ── fetch_price ───────────────────────────────────────────────────────────────

class TestFetchPrice:
    @pytest.mark.asyncio
    async def test_success_returns_float(self):
        data = {"marketdata": {"columns": ["LAST"], "data": [[142.5]]}}
        with _mock_aiohttp(200, data):
            result = await fetch_price("GAZP")
        assert result == 142.5

    @pytest.mark.asyncio
    async def test_non_200_returns_none(self):
        with _mock_aiohttp(404, {}):
            result = await fetch_price("GAZP")
        assert result is None

    @pytest.mark.asyncio
    async def test_missing_last_column_returns_none(self):
        data = {"marketdata": {"columns": ["OPEN"], "data": [[100.0]]}}
        with _mock_aiohttp(200, data):
            result = await fetch_price("GAZP")
        assert result is None

    @pytest.mark.asyncio
    async def test_null_value_returns_none(self):
        data = {"marketdata": {"columns": ["LAST"], "data": [[None]]}}
        with _mock_aiohttp(200, data):
            result = await fetch_price("GAZP")
        assert result is None

    @pytest.mark.asyncio
    async def test_empty_rows_returns_none(self):
        data = {"marketdata": {"columns": ["LAST"], "data": []}}
        with _mock_aiohttp(200, data):
            result = await fetch_price("GAZP")
        assert result is None

    @pytest.mark.asyncio
    async def test_network_exception_returns_none(self):
        import aiohttp
        with patch(
            "aiohttp.ClientSession",
            side_effect=aiohttp.ClientError("connection refused"),
        ):
            result = await fetch_price("GAZP")
        assert result is None


# ── get_correlations ──────────────────────────────────────────────────────────

class TestGetCorrelations:
    def test_empty_db_returns_empty(self, db):
        result = get_correlations(db, "rate_decision", "GAZP")
        assert result == []

    def test_empty_tickers_returns_empty(self, db):
        result = get_correlations(db, "rate_decision", "")
        assert result == []

    def test_below_min_samples_excluded(self, db):
        cid = _seed_cluster(db)
        for _ in range(_MIN_SAMPLES - 1):
            _insert_snapshot(db, cid, "SBER", "rate_decision", 300.0, 291.0)
        result = get_correlations(db, "rate_decision", "SBER")
        assert result == []

    def test_avg_calculation(self, db):
        cid = _seed_cluster(db)
        # Three snapshots: +10%, +20%, +30% → avg +20%
        for pub, post in [(100.0, 110.0), (100.0, 120.0), (100.0, 130.0)]:
            _insert_snapshot(db, cid, "GAZP", "rate_decision", pub, post)
        result = get_correlations(db, "rate_decision", "GAZP")
        assert len(result) == 1
        assert result[0].ticker == "GAZP"
        assert abs(result[0].avg_24h_pct - 20.0) < 0.01
        assert result[0].sample_count == 3

    def test_null_price_24h_not_counted(self, db):
        cid = _seed_cluster(db)
        # 2 complete + 1 pending → below _MIN_SAMPLES → excluded
        for pub, post in [(100.0, 110.0), (100.0, 120.0)]:
            _insert_snapshot(db, cid, "SBER", "rate_decision", pub, post)
        _insert_snapshot(db, cid, "SBER", "rate_decision", 100.0, None)
        result = get_correlations(db, "rate_decision", "SBER")
        assert result == []

    def test_wrong_event_type_excluded(self, db):
        cid = _seed_cluster(db)
        for _ in range(_MIN_SAMPLES):
            _insert_snapshot(db, cid, "LKOH", "sanctions", 100.0, 95.0)
        result = get_correlations(db, "rate_decision", "LKOH")
        assert result == []

    def test_multiple_tickers(self, db):
        cid = _seed_cluster(db)
        for _ in range(_MIN_SAMPLES):
            _insert_snapshot(db, cid, "GAZP", "dividends", 100.0, 105.0)
            _insert_snapshot(db, cid, "SBER", "dividends", 200.0, 190.0)

        result = get_correlations(db, "dividends", "GAZP,SBER")
        tickers = {r.ticker for r in result}
        assert tickers == {"GAZP", "SBER"}

        gazp = next(r for r in result if r.ticker == "GAZP")
        sber = next(r for r in result if r.ticker == "SBER")
        assert abs(gazp.avg_24h_pct - 5.0) < 0.01
        assert abs(sber.avg_24h_pct - (-5.0)) < 0.01

    def test_returns_ticker_correlation_objects(self, db):
        cid = _seed_cluster(db)
        for _ in range(_MIN_SAMPLES):
            _insert_snapshot(db, cid, "ROSN", "commodity_shock", 500.0, 480.0)
        result = get_correlations(db, "commodity_shock", "ROSN")
        assert len(result) == 1
        assert isinstance(result[0], TickerCorrelation)
        assert result[0].sample_count == _MIN_SAMPLES


# ── capture_price_snapshot ────────────────────────────────────────────────────

class TestCaptureSnapshot:
    @pytest.mark.asyncio
    async def test_inserts_one_row_per_ticker(self, db):
        cid = _seed_cluster(db)

        with patch("app.pipeline.price_history.get_db", return_value=db):
            with patch("app.pipeline.price_history.fetch_price", AsyncMock(return_value=150.0)):
                with patch.object(db, "close"):
                    await capture_price_snapshot(cid, "GAZP,SBER", "rate_decision")

        rows = db.execute("SELECT * FROM price_snapshots ORDER BY ticker").fetchall()
        assert len(rows) == 2
        assert rows[0]["ticker"] == "GAZP"
        assert rows[1]["ticker"] == "SBER"
        assert rows[0]["price_at_publish"] == 150.0
        assert rows[0]["event_type"] == "rate_decision"
        assert rows[0]["cluster_id"] == cid

    @pytest.mark.asyncio
    async def test_none_price_still_inserts_row(self, db):
        cid = _seed_cluster(db)

        with patch("app.pipeline.price_history.get_db", return_value=db):
            with patch("app.pipeline.price_history.fetch_price", AsyncMock(return_value=None)):
                with patch.object(db, "close"):
                    await capture_price_snapshot(cid, "GAZP", "rate_decision")

        row = db.execute("SELECT price_at_publish FROM price_snapshots LIMIT 1").fetchone()
        assert row is not None
        assert row["price_at_publish"] is None

    @pytest.mark.asyncio
    async def test_ignores_empty_tickers(self, db):
        cid = _seed_cluster(db)

        with patch("app.pipeline.price_history.get_db", return_value=db):
            with patch("app.pipeline.price_history.fetch_price", AsyncMock(return_value=100.0)):
                with patch.object(db, "close"):
                    await capture_price_snapshot(cid, "", "rate_decision")

        count = db.execute("SELECT COUNT(*) FROM price_snapshots").fetchone()["count"]
        assert count == 0


# ── update_price_snapshots ────────────────────────────────────────────────────

class TestUpdateSnapshots:
    @pytest.mark.asyncio
    async def test_fills_1h_price(self, db):
        cid = _seed_cluster(db)
        published_2h_ago = _iso(datetime.now(timezone.utc) - timedelta(hours=2))
        db.execute(
            "INSERT INTO price_snapshots (cluster_id, ticker, event_type, published_at, price_at_publish) "
            "VALUES (%s, 'GAZP', 'rate_decision', %s, 100.0)",
            (cid, published_2h_ago),
        )
        db.commit()

        with patch("app.pipeline.price_history.get_db", return_value=db):
            with patch("app.pipeline.price_history.fetch_price", AsyncMock(return_value=105.0)):
                with patch.object(db, "close"):
                    await update_price_snapshots()

        row = db.execute("SELECT price_1h, price_24h FROM price_snapshots LIMIT 1").fetchone()
        assert row["price_1h"] == 105.0
        assert row["price_24h"] is None  # not old enough for 24h yet

    @pytest.mark.asyncio
    async def test_fills_24h_price(self, db):
        cid = _seed_cluster(db)
        published_25h_ago = _iso(datetime.now(timezone.utc) - timedelta(hours=25))
        # Pre-fill price_1h so the 1h update doesn't also fire
        db.execute(
            """
            INSERT INTO price_snapshots
                (cluster_id, ticker, event_type, published_at,
                 price_at_publish, price_1h, snapshot_1h_at)
            VALUES (%s, 'SBER', 'sanctions', %s, 200.0, 198.0, %s)
            """,
            (cid, published_25h_ago, published_25h_ago),
        )
        db.commit()

        with patch("app.pipeline.price_history.get_db", return_value=db):
            with patch("app.pipeline.price_history.fetch_price", AsyncMock(return_value=195.0)):
                with patch.object(db, "close"):
                    await update_price_snapshots()

        row = db.execute("SELECT price_24h FROM price_snapshots LIMIT 1").fetchone()
        assert row["price_24h"] == 195.0

    @pytest.mark.asyncio
    async def test_skips_row_without_publish_price(self, db):
        cid = _seed_cluster(db)
        published_2h_ago = _iso(datetime.now(timezone.utc) - timedelta(hours=2))
        db.execute(
            "INSERT INTO price_snapshots (cluster_id, ticker, event_type, published_at, price_at_publish) "
            "VALUES (%s, 'GAZP', 'rate_decision', %s, NULL)",
            (cid, published_2h_ago),
        )
        db.commit()

        fetch_mock = AsyncMock(return_value=105.0)
        with patch("app.pipeline.price_history.get_db", return_value=db):
            with patch("app.pipeline.price_history.fetch_price", fetch_mock):
                with patch.object(db, "close"):
                    await update_price_snapshots()

        fetch_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_recently_published_for_1h(self, db):
        cid = _seed_cluster(db)
        # Published only 30 min ago — not old enough for 1h update
        published_30m_ago = _iso(datetime.now(timezone.utc) - timedelta(minutes=30))
        db.execute(
            "INSERT INTO price_snapshots (cluster_id, ticker, event_type, published_at, price_at_publish) "
            "VALUES (%s, 'GAZP', 'rate_decision', %s, 100.0)",
            (cid, published_30m_ago),
        )
        db.commit()

        fetch_mock = AsyncMock(return_value=105.0)
        with patch("app.pipeline.price_history.get_db", return_value=db):
            with patch("app.pipeline.price_history.fetch_price", fetch_mock):
                with patch.object(db, "close"):
                    await update_price_snapshots()

        fetch_mock.assert_not_called()


# ── _format_correlations ──────────────────────────────────────────────────────

class TestFormatCorrelations:
    def test_empty_returns_empty_string(self):
        assert _format_correlations([]) == ""

    def test_positive_value_escaped(self):
        corrs = [TickerCorrelation("GAZP", 3.1, 5)]
        result = _format_correlations(corrs)
        assert "$GAZP" in result
        assert r"\+3\.1%" in result

    def test_negative_value_escaped(self):
        corrs = [TickerCorrelation("LKOH", -2.5, 4)]
        result = _format_correlations(corrs)
        assert r"\-2\.5%" in result

    def test_truncates_to_three(self):
        corrs = [
            TickerCorrelation("A", 1.0, 3),
            TickerCorrelation("B", 2.0, 3),
            TickerCorrelation("C", 3.0, 3),
            TickerCorrelation("D", 4.0, 3),
        ]
        result = _format_correlations(corrs)
        assert "$A" in result
        assert "$B" in result
        assert "$C" in result
        assert "$D" not in result

    def test_n_shows_max_sample_count(self):
        corrs = [
            TickerCorrelation("GAZP", 1.0, 6),
            TickerCorrelation("SBER", 2.0, 3),
        ]
        result = _format_correlations(corrs)
        assert "n\\=6" in result

    def test_line_starts_with_prefix(self):
        corrs = [TickerCorrelation("GAZP", 5.0, 4)]
        result = _format_correlations(corrs)
        assert result.startswith("📊 По истории:")

    def test_single_item_includes_n_and_24h(self):
        corrs = [TickerCorrelation("SBER", -1.2, 7)]
        result = _format_correlations(corrs)
        assert "за 24ч" in result
        assert "n\\=7" in result
