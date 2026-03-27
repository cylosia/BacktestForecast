from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from html import unescape


@dataclass(frozen=True, slots=True)
class MassiveIncident:
    name: str
    status: str
    created_at: str | None = None


@dataclass(frozen=True, slots=True)
class MassiveStatusSummary:
    source: str
    overall_status: str | None = None
    options_status: str | None = None
    options_market_data_rest_status: str | None = None
    active_incidents: list[MassiveIncident] = field(default_factory=list)

    @property
    def options_rest_degraded(self) -> bool:
        value = (self.options_market_data_rest_status or "").strip().lower()
        return value not in {"", "operational"}

    def to_dict(self) -> dict[str, object]:
        return {
            "source": self.source,
            "overall_status": self.overall_status,
            "options_status": self.options_status,
            "options_market_data_rest_status": self.options_market_data_rest_status,
            "options_rest_degraded": self.options_rest_degraded,
            "active_incidents": [
                {
                    "name": incident.name,
                    "status": incident.status,
                    "created_at": incident.created_at,
                }
                for incident in self.active_incidents
            ],
        }


def _request_text(url: str, *, timeout_seconds: int) -> str:
    req = urllib.request.Request(
        url,
        headers={"Accept": "application/json, text/html;q=0.9"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout_seconds) as response:  # noqa: S310
        return response.read().decode("utf-8", errors="replace")


def parse_summary_json(raw: str) -> MassiveStatusSummary:
    payload = json.loads(raw)
    components = payload.get("components", [])
    incidents_payload = payload.get("incidents", [])

    overall = payload.get("status", {}).get("indicator")
    options_status = None
    options_market_data_rest_status = None
    options_group_id = None

    for component in components:
        name = component.get("name")
        status = component.get("status")
        if name == "Options":
            options_status = status
            options_group_id = component.get("id")
            break

    for component in components:
        name = component.get("name")
        group_id = component.get("group_id")
        status = component.get("status")
        if name == "Market Data REST Endpoints" and group_id == options_group_id:
            options_market_data_rest_status = status
            break

    incidents = [
        MassiveIncident(
            name=str(incident.get("name") or ""),
            status=str(incident.get("status") or ""),
            created_at=incident.get("created_at"),
        )
        for incident in incidents_payload
        if incident.get("status") not in {"resolved", "completed"}
    ]

    return MassiveStatusSummary(
        source="statuspage_json",
        overall_status=overall,
        options_status=options_status,
        options_market_data_rest_status=options_market_data_rest_status,
        active_incidents=incidents,
    )


def parse_status_page_html(raw_html: str) -> MassiveStatusSummary:
    text = unescape(re.sub(r"<[^>]+>", "\n", raw_html))
    lines = [line.strip() for line in text.splitlines() if line.strip()]

    options_status = None
    options_market_data_rest_status = None
    overall_status = None
    active_incidents: list[MassiveIncident] = []

    for idx, line in enumerate(lines):
        if line == "Options" and idx + 1 < len(lines):
            options_status = lines[idx + 1]
            break

    if options_status is not None:
        try:
            start = lines.index("Options")
            end = len(lines)
            for candidate in ("Indices", "Forex", "Crypto", "Futures (Beta)"):
                with_context = [i for i, line in enumerate(lines[start + 1 :], start + 1) if line == candidate]
                if with_context:
                    end = min(end, with_context[0])
            options_lines = lines[start:end]
            for idx, line in enumerate(options_lines):
                if line == "Market Data REST Endpoints" and idx + 1 < len(options_lines):
                    options_market_data_rest_status = options_lines[idx + 1]
                    break
        except ValueError:
            pass

    indicators = {"Operational", "Degraded Performance", "Partial Outage", "Major Outage", "Maintenance"}
    if lines:
        for line in lines:
            if line in indicators:
                overall_status = line
                break

    for idx, line in enumerate(lines):
        if "Elevated Latency and Timeouts" in line and idx + 1 < len(lines):
            status_line = lines[idx + 1]
            if status_line in {"Investigating", "Identified", "Monitoring"}:
                created_at = lines[idx + 2] if idx + 2 < len(lines) else None
                active_incidents.append(
                    MassiveIncident(name=line, status=status_line, created_at=created_at)
                )

    return MassiveStatusSummary(
        source="statuspage_html",
        overall_status=overall_status,
        options_status=options_status,
        options_market_data_rest_status=options_market_data_rest_status,
        active_incidents=active_incidents,
    )


def fetch_massive_status(
    *,
    base_url: str = "https://massive-status.com",
    timeout_seconds: int = 10,
) -> MassiveStatusSummary:
    errors: list[str] = []
    for path in ("/api/v2/summary.json", ""):
        url = f"{base_url.rstrip('/')}{path}"
        try:
            raw = _request_text(url, timeout_seconds=timeout_seconds)
            if path.endswith(".json"):
                return parse_summary_json(raw)
            return parse_status_page_html(raw)
        except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
            errors.append(f"{path or '/'}: {exc}")
            continue
    raise RuntimeError("Failed to fetch Massive status: " + "; ".join(errors))
