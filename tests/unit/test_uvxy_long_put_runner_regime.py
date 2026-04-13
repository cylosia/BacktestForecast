from __future__ import annotations

import importlib.util
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path


def _load_runner_module():
    project_root = Path(__file__).resolve().parents[2]
    script_path = project_root / "scripts" / "run_uvxy_2015_long_put_grid_optimized.py"
    spec = importlib.util.spec_from_file_location("uvxy_long_put_grid_runner", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(script_path.parent))
    try:
        spec.loader.exec_module(module)
    finally:
        if sys.path and sys.path[0] == str(script_path.parent):
            sys.path.pop(0)
    return module


def test_runner_parse_args_defaults_include_new_window_and_regime_flags() -> None:
    module = _load_runner_module()

    args = module._parse_args([])

    assert args.start_date == "2015-05-04"
    assert args.end_date == "2020-04-30"
    assert args.require_regime == []
    assert args.block_regime == []
    assert "required_regimes" in module.CSV_FIELDS
    assert "blocked_regimes" in module.CSV_FIELDS
    assert "eligible_entry_days" in module.CSV_FIELDS


def test_runner_build_request_includes_regime_rule() -> None:
    module = _load_runner_module()

    entry_rules = module._build_regime_entry_rules(
        required_labels=["bearish", "trending"],
        blocked_labels=["high_iv"],
    )
    request = module._build_request(
        symbol="UVXY",
        strategy_type=module.StrategyType.LONG_PUT,
        start_date=date(2015, 5, 4),
        end_date=date(2020, 4, 30),
        target_dte=7,
        dte_tolerance_days=0,
        max_holding_days=120,
        account_size=Decimal("100000"),
        risk_per_trade_pct=Decimal("100"),
        commission_per_contract=Decimal("0.65"),
        slippage_pct=Decimal("0"),
        delta_target=25,
        entry_rules=entry_rules,
    )

    assert len(request.entry_rules) == 1
    rule = request.entry_rules[0]
    assert rule.type == "regime"
    assert [value.value for value in rule.required_regimes] == ["bearish", "trending"]
    assert [value.value for value in rule.blocked_regimes] == ["high_iv"]
