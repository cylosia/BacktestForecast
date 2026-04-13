from __future__ import annotations

import importlib.util
import json
import sys
import uuid
from contextlib import ExitStack
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from backtestforecast.db.base import Base
from backtestforecast.models import HistoricalUnderlyingDayBar


def _load_script_module():
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "optimize_underlying_rotation.py"
    sys.path.insert(0, str(script_path.parent))
    try:
        spec = importlib.util.spec_from_file_location("optimize_underlying_rotation", script_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.pop(0)


def _seed_sqlite_database(db_path: Path) -> None:
    engine = create_engine(f"sqlite:///{db_path}", future=True)
    try:
        Base.metadata.create_all(engine)
        factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
        with factory() as session:
            start = date(2020, 1, 1)
            rows = {
                "A": [100, 150, 150, 150, 140, 130, 120, 110],
                "B": [100, 110, 120, 130, 140, 150, 160, 170],
            }
            for symbol, closes in rows.items():
                for offset, close_price in enumerate(closes):
                    trade_date = start + timedelta(days=offset)
                    session.add(
                        HistoricalUnderlyingDayBar(
                            id=uuid.uuid4(),
                            symbol=symbol,
                            trade_date=trade_date,
                            open_price=Decimal(str(close_price)),
                            high_price=Decimal(str(close_price)),
                            low_price=Decimal(str(close_price)),
                            close_price=Decimal(str(close_price)),
                            volume=Decimal("1000000"),
                            source_dataset="test",
                            source_file_date=trade_date,
                        )
                    )
            session.commit()
    finally:
        engine.dispose()


def test_optimize_underlying_rotation_script_writes_result_json() -> None:
    module = _load_script_module()
    unique = uuid.uuid4().hex
    db_path = Path(f"rotation-test-{unique}.sqlite")
    output_path = Path(f"rotation-test-{unique}.json")
    try:
        _seed_sqlite_database(db_path)

        argv = [
            "optimize_underlying_rotation.py",
            "--database-url",
            f"sqlite:///{db_path}",
            "--train-start",
            "2020-01-01",
            "--train-end",
            "2020-01-06",
            "--validation-start",
            "2020-01-07",
            "--validation-end",
            "2020-01-08",
            "--portfolio-sizes",
            "1",
            "--lookback-triplets",
            "1:2:3",
            "--weight-triplets",
            "1:0:0",
            "--trailing-stop-pcts",
            "0",
            "--rebalance-frequencies",
            "1",
            "--min-training-bars",
            "2",
            "--min-training-avg-dollar-volume",
            "1",
            "--min-training-close-price",
            "1",
            "--output-json",
            str(output_path),
        ]

        with ExitStack() as stack:
            stack.enter_context(patch.object(sys, "argv", argv))
            result = module.main()

        payload = json.loads(output_path.read_text(encoding="utf-8"))
        assert result == 0
        assert payload["candidate_count"] == 1
        assert payload["universe_size"] == 2
        assert payload["best_config"]["portfolio_size"] == 1
        assert payload["top_rows"][0]["train_summary"]["total_roi_pct"] > 0
    finally:
        output_path.unlink(missing_ok=True)
        db_path.unlink(missing_ok=True)
