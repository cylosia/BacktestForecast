from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
import uuid

from backtestforecast.models import BacktestRun, ExportJob, ScannerJob, SweepJob, SymbolAnalysis
from backtestforecast.utils import decode_cursor, encode_cursor
from tests.integration.test_endpoint_coverage import _ensure_user, _set_user_plan


SAME_TIME = datetime(2025, 3, 10, 12, 0, 0, tzinfo=UTC)
OLDER_TIME = SAME_TIME - timedelta(minutes=1)


def _create_backtest_run(user_id, *, created_at: datetime, symbol: str) -> BacktestRun:
    run = BacktestRun(
        user_id=user_id,
        status="succeeded",
        symbol=symbol,
        strategy_type="long_call",
        date_from=date(2024, 1, 1),
        date_to=date(2024, 6, 1),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=20,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("2.0"),
        commission_per_contract=Decimal("0.65"),
        completed_at=created_at,
    )
    run.created_at = created_at
    return run


def _page_ids(client, path: str, auth_headers: dict[str, str], *, limit: int = 2) -> list[str]:
    seen: list[str] = []
    cursor = None
    for _ in range(5):
        params = {"limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        resp = client.get(path, params=params, headers=auth_headers)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        seen.extend(item["id"] for item in body["items"])
        cursor = body.get("next_cursor")
        if not cursor:
            break
    return seen


def test_encode_decode_cursor_round_trip_uses_datetime_and_id():
    row_id = uuid.uuid4()
    cursor = encode_cursor(SAME_TIME, row_id)
    decoded = decode_cursor(cursor)
    assert decoded == (SAME_TIME, row_id)



def test_backtests_cursor_pagination_handles_same_timestamp_records(client, auth_headers, db_session):
    _ensure_user(client, auth_headers)
    user = _set_user_plan(db_session)
    runs = [
        _create_backtest_run(user.id, created_at=SAME_TIME, symbol="AAA"),
        _create_backtest_run(user.id, created_at=SAME_TIME, symbol="BBB"),
        _create_backtest_run(user.id, created_at=SAME_TIME, symbol="CCC"),
        _create_backtest_run(user.id, created_at=OLDER_TIME, symbol="DDD"),
    ]
    db_session.add_all(runs)
    db_session.commit()

    seen = _page_ids(client, "/v1/backtests", auth_headers)
    expected = {str(run.id) for run in runs}
    assert set(seen) == expected
    assert len(seen) == len(expected)



def test_scans_cursor_pagination_handles_same_timestamp_records(client, auth_headers, db_session):
    _ensure_user(client, auth_headers)
    user = _set_user_plan(db_session)
    jobs = [
        ScannerJob(user_id=user.id, status="succeeded", mode="basic", plan_tier_snapshot="pro", request_hash=f"scan-{idx}")
        for idx in range(4)
    ]
    for idx, job in enumerate(jobs):
        job.created_at = SAME_TIME if idx < 3 else OLDER_TIME
        job.completed_at = job.created_at
    db_session.add_all(jobs)
    db_session.commit()

    seen = _page_ids(client, "/v1/scans", auth_headers)
    expected = {str(job.id) for job in jobs}
    assert set(seen) == expected
    assert len(seen) == len(expected)



def test_sweeps_cursor_pagination_handles_same_timestamp_records(client, auth_headers, db_session):
    _ensure_user(client, auth_headers)
    user = _set_user_plan(db_session)
    jobs = [
        SweepJob(user_id=user.id, status="succeeded", symbol=f"SYM{idx}", mode="grid", plan_tier_snapshot="pro")
        for idx in range(4)
    ]
    for idx, job in enumerate(jobs):
        job.created_at = SAME_TIME if idx < 3 else OLDER_TIME
        job.completed_at = job.created_at
    db_session.add_all(jobs)
    db_session.commit()

    seen = _page_ids(client, "/v1/sweeps", auth_headers)
    expected = {str(job.id) for job in jobs}
    assert set(seen) == expected
    assert len(seen) == len(expected)



def test_exports_cursor_pagination_handles_same_timestamp_records(client, auth_headers, db_session):
    _ensure_user(client, auth_headers)
    user = _set_user_plan(db_session)
    run = _create_backtest_run(user.id, created_at=OLDER_TIME - timedelta(minutes=1), symbol="EXP")
    db_session.add(run)
    db_session.flush()
    jobs = [
        ExportJob(
            user_id=user.id,
            backtest_run_id=run.id,
            export_format="csv",
            status="succeeded",
            file_name=f"export-{idx}.csv",
            mime_type="text/csv",
            content_bytes=b"id\n1\n",
            expires_at=SAME_TIME + timedelta(days=30),
        )
        for idx in range(4)
    ]
    for idx, job in enumerate(jobs):
        job.created_at = SAME_TIME if idx < 3 else OLDER_TIME
        job.completed_at = job.created_at
    db_session.add_all(jobs)
    db_session.commit()

    seen = _page_ids(client, "/v1/exports", auth_headers)
    expected = {str(job.id) for job in jobs}
    assert set(seen) == expected
    assert len(seen) == len(expected)



def test_analysis_cursor_pagination_handles_same_timestamp_records(client, auth_headers, db_session):
    _ensure_user(client, auth_headers)
    user = _set_user_plan(db_session)
    analyses = [
        SymbolAnalysis(user_id=user.id, symbol=f"AN{idx}", status="succeeded", stage="forecast")
        for idx in range(4)
    ]
    for idx, analysis in enumerate(analyses):
        analysis.created_at = SAME_TIME if idx < 3 else OLDER_TIME
        analysis.completed_at = analysis.created_at
    db_session.add_all(analyses)
    db_session.commit()

    seen = _page_ids(client, "/v1/analysis", auth_headers)
    expected = {str(analysis.id) for analysis in analyses}
    assert set(seen) == expected
    assert len(seen) == len(expected)
