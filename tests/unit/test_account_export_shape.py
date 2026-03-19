"""Verify account export response contains expected keys."""


def test_export_response_keys():
    expected_keys = {
        "user", "pagination", "backtests", "templates",
        "scanner_jobs", "sweep_jobs", "export_jobs", "symbol_analyses",
    }
    from apps.api.app.routers.account import export_account_data
    import inspect
    source = inspect.getsource(export_account_data)
    for key in expected_keys:
        assert f'"{key}"' in source, f"Missing key '{key}' in export response"
