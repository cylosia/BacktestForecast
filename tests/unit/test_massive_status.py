from __future__ import annotations

from backtestforecast.integrations.massive_status import (
    fetch_massive_status,
    parse_status_page_html,
    parse_summary_json,
)


def test_parse_summary_json_extracts_options_rest_status():
    raw = """
    {
      "status": {"indicator": "minor"},
      "components": [
        {"id": "stocks", "name": "Stocks", "status": "operational", "group": false},
        {"id": "options", "name": "Options", "status": "partial_outage", "group": true},
        {"id": "options-rest", "name": "Market Data REST Endpoints", "status": "partial_outage", "group_id": "options"},
        {"id": "options-ref", "name": "Reference Data REST Endpoints", "status": "operational", "group_id": "options"}
      ],
      "incidents": [
        {"name": "Options Endpoints – Elevated Latency and Timeouts", "status": "investigating", "created_at": "2026-03-27T13:56:00Z"},
        {"name": "Old incident", "status": "resolved", "created_at": "2026-03-20T13:56:00Z"}
      ]
    }
    """

    summary = parse_summary_json(raw)

    assert summary.source == "statuspage_json"
    assert summary.overall_status == "minor"
    assert summary.options_status == "partial_outage"
    assert summary.options_market_data_rest_status == "partial_outage"
    assert summary.options_rest_degraded is True
    assert len(summary.active_incidents) == 1
    assert summary.active_incidents[0].name == "Options Endpoints – Elevated Latency and Timeouts"


def test_parse_status_page_html_extracts_current_incident():
    raw = """
    <html><body>
    <div>Options</div>
    <div>Partial Outage</div>
    <div>Market Data REST Endpoints</div>
    <div>Partial Outage</div>
    <div>Reference Data REST Endpoints</div>
    <div>Operational</div>
    <div>Indices</div>
    <div>Operational</div>
    <div>Options Endpoints – Elevated Latency and Timeouts</div>
    <div>Investigating</div>
    <div>Mar 27, 2026 - 09:56 EDT</div>
    </body></html>
    """

    summary = parse_status_page_html(raw)

    assert summary.source == "statuspage_html"
    assert summary.options_status == "Partial Outage"
    assert summary.options_market_data_rest_status == "Partial Outage"
    assert summary.options_rest_degraded is True
    assert len(summary.active_incidents) == 1
    assert summary.active_incidents[0].status == "Investigating"


def test_fetch_massive_status_falls_back_to_html(monkeypatch):
    calls: list[str] = []

    def _fake_request(url: str, *, timeout_seconds: int) -> str:
        calls.append(url)
        if url.endswith("/api/v2/summary.json"):
            raise ValueError("bad json")
        return """
        <html><body>
        <div>Options</div>
        <div>Operational</div>
        <div>Market Data REST Endpoints</div>
        <div>Operational</div>
        <div>Indices</div>
        <div>Operational</div>
        </body></html>
        """

    monkeypatch.setattr("backtestforecast.integrations.massive_status._request_text", _fake_request)

    summary = fetch_massive_status()

    assert len(calls) == 2
    assert summary.source == "statuspage_html"
    assert summary.options_rest_degraded is False
