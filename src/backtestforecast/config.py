from __future__ import annotations

import threading
from collections.abc import Callable
from typing import ClassVar

import structlog
from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = structlog.get_logger(__name__)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)

    app_name: str = "BacktestForecast API"
    app_env: str = "development"
    app_public_url: str = "http://localhost:3000"
    api_public_url: str = "http://localhost:8000"
    api_port: int = 8000
    web_port: int = 3000
    log_level: str = "INFO"
    log_json: bool = False

    database_url: str = Field(
        default="postgresql+psycopg://backtestforecast:backtestforecast@localhost:5432/backtestforecast",
        repr=False,
    )
    # Optional read-replica URL for read-heavy API endpoints (list, compare,
    # recommendations).  When set, get_readonly_db() returns a session bound
    # to this URL.  When unset, read-only endpoints use the primary.
    database_read_replica_url: str | None = Field(default=None, repr=False)
    redis_url: str = Field(default="redis://localhost:6379/0", repr=False)
    redis_password: str | None = Field(default=None, repr=False)
    # Separate Redis URL for non-broker usage (rate limiting, SSE pub/sub, caching).
    # Defaults to redis_url when not set.  In production, point this at a dedicated
    # Redis instance or database number to isolate cache/SSE traffic from Celery broker traffic.
    redis_cache_url: str | None = Field(default=None, repr=False)
    # Separate Redis URL for Celery result backend.  Defaults to redis_url.
    # In production, use a different Redis database (e.g. redis://host:6379/2)
    # to isolate result storage from broker message queues and prevent
    # contention under high task throughput.
    celery_result_backend_url: str | None = Field(default=None, repr=False)

    web_cors_origins_raw: str = "http://localhost:3000"
    api_allowed_hosts_raw: str = "localhost,127.0.0.1"
    request_max_body_bytes: int = 1_048_576
    request_timeout_seconds: int = 60

    audit_cleanup_enabled: bool = True
    audit_cleanup_retention_days: int = Field(default=90, ge=7, le=730)

    clerk_secret_key: str | None = Field(default=None, repr=False)
    clerk_issuer: str | None = None
    clerk_audience: str | None = None
    clerk_jwks_url: str | None = None
    clerk_jwt_key: str | None = Field(default=None, repr=False)
    clerk_jwks_fetch_timeout: float = 10.0
    jwt_leeway_seconds: int = Field(
        default=10,
        ge=0,
        le=120,
        description="Clock skew tolerance for JWT exp/nbf claims. "
                    "Cloud VMs can drift 5-15s during suspend/resume. Default 10s.",
    )
    clerk_authorized_parties_raw: str = Field(default="http://localhost:3000")

    stripe_secret_key: str | None = Field(default=None, repr=False)
    stripe_webhook_secret: str | None = Field(default=None, repr=False)
    stripe_pro_monthly_price_id: str | None = None
    stripe_pro_yearly_price_id: str | None = None
    stripe_premium_monthly_price_id: str | None = None
    stripe_premium_yearly_price_id: str | None = None
    stripe_circuit_cooldown_seconds: int = 30

    massive_api_key: str | None = Field(default=None, repr=False)
    massive_base_url: str = "https://api.massive.com"
    massive_timeout_seconds: float = 30.0
    massive_max_retries: int = 2
    massive_retry_backoff_seconds: float = 0.5
    earnings_api_key: str | None = Field(default=None, repr=False)

    option_cache_enabled: bool = True
    option_cache_ttl_seconds: int = 604_800  # 7 days
    option_cache_warn_age_seconds: int = Field(
        default=259_200, ge=3600,
        description="Log a warning when cached option data is older than this (default 3 days).",
    )
    prefetch_max_workers: int = Field(default=10, ge=1, le=32)

    # Nightly pipeline - override via PIPELINE_DEFAULT_SYMBOLS_CSV env var
    pipeline_default_symbols_csv: str | None = None
    pipeline_default_symbols: list[str] = [
        "AAPL",
        "MSFT",
        "AMZN",
        "GOOGL",
        "META",
        "NVDA",
        "TSLA",
        "AMD",
        "NFLX",
        "CRM",
        "ORCL",
        "INTC",
        "QCOM",
        "AVGO",
        "ADBE",
        "CSCO",
        "TXN",
        "MU",
        "AMAT",
        "LRCX",
        "SPY",
        "QQQ",
        "IWM",
        "DIA",
        "XLF",
        "XLE",
        "XLK",
        "XLV",
        "XLI",
        "XLP",
        "JPM",
        "BAC",
        "GS",
        "MS",
        "C",
        "WFC",
        "V",
        "MA",
        "AXP",
        "PYPL",
        "JNJ",
        "UNH",
        "PFE",
        "ABBV",
        "MRK",
        "LLY",
        "TMO",
        "ABT",
        "BMY",
        "AMGN",
        "XOM",
        "CVX",
        "COP",
        "SLB",
        "EOG",
        "PSX",
        "MPC",
        "VLO",
        "OXY",
        "HAL",
        "BA",
        "CAT",
        "DE",
        "HON",
        "UPS",
        "FDX",
        "LMT",
        "RTX",
        "GE",
        "MMM",
        "WMT",
        "COST",
        "TGT",
        "HD",
        "LOW",
        "SBUX",
        "MCD",
        "NKE",
        "PG",
        "KO",
        "DIS",
        "CMCSA",
        "T",
        "VZ",
        "TMUS",
        "CHTR",
        "ROKU",
        "SNAP",
        "SQ",
        "COIN",
        "PLTR",
        "SOFI",
        "RIVN",
        "LCID",
        "NIO",
        "MARA",
        "RIOT",
        "HOOD",
        "DKNG",
        "ABNB",
    ]

    sentry_dsn: str | None = None
    sentry_traces_sample_rate: float = 0.1

    metrics_token: str | None = None
    admin_token: str | None = None

    ip_hash_salt: str = Field(default="backtestforecast-default-ip-salt-change-me")

    db_pool_size: int = Field(default=5, ge=1, le=100)
    db_pool_max_overflow: int = Field(default=10, ge=0, le=100)
    db_pool_recycle: int = Field(default=1800, ge=60, le=7200)
    db_pool_timeout: int = Field(default=10, ge=1, le=120)
    db_statement_timeout_ms: int = Field(default=30_000, ge=1000)
    db_worker_statement_timeout_ms: int = Field(default=300_000, ge=1000)

    trusted_proxy_cidrs: str = "127.0.0.0/8"

    rate_limit_prefix: str = "bff:rate-limit"
    rate_limit_fail_closed: bool = True
    rate_limit_degraded_memory_fallback: bool = False
    rate_limit_memory_max_keys: int = 10_000
    backtest_create_rate_limit: int = 10
    backtest_read_rate_limit: int = 60
    me_read_rate_limit: int = 60
    scan_create_rate_limit: int = 6
    scan_read_rate_limit: int = 60
    sweep_create_rate_limit: int = 3
    sweep_read_rate_limit: int = 60
    export_create_rate_limit: int = 20
    export_read_rate_limit: int = 60
    billing_create_rate_limit: int = 10
    template_mutate_rate_limit: int = 20
    analysis_create_rate_limit: int = 10
    analysis_read_rate_limit: int = 60
    delete_rate_limit: int = 60
    analysis_rate_limit_window_seconds: int = 3600
    forecast_rate_limit: int = 6
    daily_picks_rate_limit: int = 30
    daily_picks_pipeline_hour_utc: int = Field(default=6, ge=0, le=23)
    daily_picks_pipeline_minute_utc: int = Field(default=0, ge=0, le=59)
    sse_rate_limit: int = 30
    sse_redis_max_connections: int = 50
    sse_redis_socket_timeout: float = 10.0
    sse_redis_connect_timeout: float = 5.0
    redis_reconnect_backoff_seconds: float = 30.0
    sse_max_payload_bytes: int = 10_000
    rate_limit_window_seconds: int = 60

    pipeline_max_workers: int = Field(default=20, ge=1, le=64)

    scan_timeout_seconds: int = Field(
        default=480,
        ge=120,
        description="Scan-internal timeout. Must be at least 120s below the Celery "
                    "soft_time_limit (600s) to allow cleanup on timeout.",
    )
    sweep_timeout_seconds: int = Field(default=3600, ge=1)
    sweep_genetic_timeout_seconds: int = Field(default=3600, ge=1)
    max_concurrent_sweeps: int = Field(default=10, ge=1, le=100)

    sweep_score_min_trades: int = Field(default=3, ge=1, le=50)
    sweep_score_win_rate_weight: float = 0.25
    sweep_score_roi_weight: float = 0.35
    sweep_score_sharpe_weight: float = 0.20
    sweep_score_sharpe_multiplier: float = 2.0
    sweep_score_drawdown_weight: float = 0.20

    max_scan_equity_points: int = Field(default=500, ge=10, le=5000)
    max_pdf_trades: int = Field(default=100, ge=10, le=1000)

    max_concurrent_analyses_default: int = Field(default=3, ge=1, le=20)
    max_concurrent_analyses_premium: int = Field(default=5, ge=1, le=20)

    forecast_max_analogs: int = Field(default=20, ge=1)

    active_renewal_grace_hours: int = 72
    past_due_grace_days: int = 7
    max_reconciliation_users: int = Field(default=100, ge=1, le=500)

    risk_free_rate: float = 0.045
    fallback_entry_rule_rsi_threshold: int = Field(default=40, ge=1, le=100)

    max_backtest_window_days: int = 1_825
    max_scanner_window_days: int = 730
    max_sweep_window_days: int = 730

    s3_bucket: str | None = None
    s3_region: str | None = None
    s3_endpoint_url: str | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = Field(default=None, repr=False)

    # Runtime feature flags - toggle via env vars without code deployment.
    #
    # Each feature supports four layers of control (see feature_flags.py):
    #   1. Kill-switch: feature_{name}_enabled = False -> disabled for all
    #   2. Allow-list: feature_{name}_allow_user_ids = "uuid1,uuid2"
    #   3. Tier targeting: feature_{name}_tiers = "pro,premium"
    #   4. Percentage rollout: feature_{name}_rollout_pct = 50
    #
    # Use `from backtestforecast.feature_flags import is_feature_enabled`
    # for user-aware checks.  The boolean flags below remain the kill-switch.
    feature_backtests_enabled: bool = True
    feature_scanner_enabled: bool = True
    feature_exports_enabled: bool = True
    feature_forecasts_enabled: bool = True
    feature_analysis_enabled: bool = True
    feature_daily_picks_enabled: bool = True
    feature_billing_enabled: bool = True
    feature_sweeps_enabled: bool = True

    # Percentage rollout (0-100). 100 = enabled for all users.
    feature_sweeps_rollout_pct: int = Field(default=100, ge=0, le=100)
    feature_analysis_rollout_pct: int = Field(default=100, ge=0, le=100)

    # Tier targeting (comma-separated). Empty = all tiers allowed.
    feature_sweeps_tiers: str = ""
    feature_analysis_tiers: str = ""

    # Allow-list override (comma-separated UUIDs). Empty = no overrides.
    feature_sweeps_allow_user_ids: str = ""
    feature_analysis_allow_user_ids: str = ""

    @field_validator("app_env")
    @classmethod
    def normalize_app_env(cls, value: str) -> str:
        normalized = value.strip().lower()
        valid_envs = {"development", "test", "staging", "production"}
        if normalized and normalized not in valid_envs:
            raise ValueError(f"app_env must be one of {valid_envs}, got '{normalized}'")
        return normalized or "development"

    @field_validator("log_level")
    @classmethod
    def normalize_log_level(cls, value: str) -> str:
        normalized = value.strip().upper()
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if normalized and normalized not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}, got '{normalized}'")
        return normalized or "INFO"

    @field_validator(
        "request_max_body_bytes", "max_backtest_window_days", "max_scanner_window_days", "max_sweep_window_days",
        "rate_limit_window_seconds", "db_pool_size", "db_pool_recycle",
        "analysis_rate_limit_window_seconds",
        "backtest_create_rate_limit", "backtest_read_rate_limit", "me_read_rate_limit",
        "scan_create_rate_limit", "scan_read_rate_limit",
        "sweep_create_rate_limit", "sweep_read_rate_limit",
        "export_create_rate_limit", "export_read_rate_limit",
        "billing_create_rate_limit",
        "template_mutate_rate_limit",
        "analysis_create_rate_limit", "analysis_read_rate_limit", "delete_rate_limit",
        "forecast_rate_limit", "daily_picks_rate_limit",
        "rate_limit_memory_max_keys",
        "sse_rate_limit", "sse_redis_max_connections", "sse_max_payload_bytes",
        "scan_timeout_seconds",
        "forecast_max_analogs",
        "option_cache_ttl_seconds",
        "request_timeout_seconds",
        "active_renewal_grace_hours",
        "past_due_grace_days",
        "stripe_circuit_cooldown_seconds",
    )
    @classmethod
    def validate_positive_ints(cls, value: int) -> int:
        v = int(value)
        if v < 1:
            raise ValueError(f"Value must be >= 1, got {v}")
        return v

    @field_validator(
        "sweep_score_win_rate_weight", "sweep_score_roi_weight",
        "sweep_score_sharpe_weight", "sweep_score_drawdown_weight",
    )
    @classmethod
    def validate_sweep_score_weight_range(cls, value: float) -> float:
        v = float(value)
        if v < 0.0 or v > 1.0:
            raise ValueError(f"Sweep score weight must be between 0.0 and 1.0, got {v}")
        return v

    @field_validator("sweep_score_sharpe_multiplier")
    @classmethod
    def validate_non_negative_float(cls, value: float) -> float:
        v = float(value)
        if v < 0.0:
            raise ValueError(f"Value must be >= 0, got {v}")
        return v

    @field_validator("massive_timeout_seconds", "sse_redis_socket_timeout", "sse_redis_connect_timeout", "clerk_jwks_fetch_timeout")
    @classmethod
    def validate_positive_floats(cls, value: float) -> float:
        v = float(value)
        if v < 0.1:
            raise ValueError(f"Value must be >= 0.1, got {v}")
        return v

    @field_validator("sentry_traces_sample_rate")
    @classmethod
    def validate_sample_rate(cls, value: float) -> float:
        v = float(value)
        if v < 0.0 or v > 1.0:
            raise ValueError(f"sentry_traces_sample_rate must be between 0.0 and 1.0, got {v}")
        return v

    @field_validator("massive_max_retries")
    @classmethod
    def validate_retry_count(cls, value: int) -> int:
        v = int(value)
        if v < 0:
            raise ValueError(f"massive_max_retries must be >= 0, got {v}")
        return v

    @field_validator("massive_retry_backoff_seconds", "redis_reconnect_backoff_seconds")
    @classmethod
    def validate_positive_backoff(cls, value: float) -> float:
        v = float(value)
        if v <= 0.0:
            raise ValueError(f"Value must be > 0, got {v}")
        return v

    @field_validator("risk_free_rate")
    @classmethod
    def validate_risk_free_rate(cls, value: float) -> float:
        v = float(value)
        if v < 0.0 or v > 0.20:
            raise ValueError(f"risk_free_rate must be between 0.0 and 0.20, got {v}")
        return v

    @field_validator("api_port", "web_port")
    @classmethod
    def validate_port_range(cls, value: int) -> int:
        v = int(value)
        if v < 1 or v > 65535:
            raise ValueError(f"Port must be between 1 and 65535, got {v}")
        return v

    @field_validator("ip_hash_salt")
    @classmethod
    def validate_ip_hash_salt_length(cls, value: str) -> str:
        if len(value) < 16:
            raise ValueError(
                f"IP_HASH_SALT is only {len(value)} characters; "
                "use at least 16 characters for adequate entropy."
            )
        return value

    @field_validator("trusted_proxy_cidrs")
    @classmethod
    def validate_trusted_proxy_cidrs(cls, value: str) -> str:
        import ipaddress
        for entry in value.split(","):
            entry = entry.strip()
            if entry:
                try:
                    ipaddress.ip_network(entry, strict=False)
                except ValueError as exc:
                    raise ValueError(f"Invalid CIDR in trusted_proxy_cidrs: {entry!r}") from exc
        return value

    @property
    def web_cors_origins(self) -> list[str]:
        # NOTE: This re-parses on every access. Acceptable since Settings is a
        # cached singleton and the parse is trivially fast.
        raw = self.web_cors_origins_raw.strip()
        if not raw:
            return []
        origins: list[str] = []
        for origin in raw.split(","):
            origin = origin.strip()
            if not origin:
                continue
            if origin != "*" and not origin.startswith("http://") and not origin.startswith("https://"):
                logger.warning("Skipping invalid CORS origin (must start with http:// or https://): %s", origin)
                continue
            origins.append(origin)
        if not origins and raw:
            raise ValueError(
                f"WEB_CORS_ORIGINS_RAW is set to '{raw}' but all entries were invalid. "
                "Each origin must start with http:// or https://, or be '*'."
            )
        return origins

    @property
    def api_allowed_hosts(self) -> list[str]:
        # NOTE: This re-parses on every access. Acceptable since Settings is a
        # cached singleton and the parse is trivially fast.
        hosts = [host.strip() for host in self.api_allowed_hosts_raw.split(",") if host.strip()]
        return hosts or ["localhost", "127.0.0.1"]

    @property
    def clerk_authorized_parties(self) -> list[str]:
        # NOTE: This re-parses on every access. Acceptable since Settings is a
        # cached singleton and the parse is trivially fast.
        return [party.strip() for party in self.clerk_authorized_parties_raw.split(",") if party.strip()]

    @property
    def stripe_price_lookup(self) -> dict[tuple[str, str], str]:
        # NOTE: This re-parses on every access. Acceptable since Settings is a
        # cached singleton and the parse is trivially fast.
        mapping: dict[tuple[str, str], str] = {}
        if self.stripe_pro_monthly_price_id:
            mapping[("pro", "monthly")] = self.stripe_pro_monthly_price_id
        if self.stripe_pro_yearly_price_id:
            mapping[("pro", "yearly")] = self.stripe_pro_yearly_price_id
        if self.stripe_premium_monthly_price_id:
            mapping[("premium", "monthly")] = self.stripe_premium_monthly_price_id
        if self.stripe_premium_yearly_price_id:
            mapping[("premium", "yearly")] = self.stripe_premium_yearly_price_id
        return mapping

    @property
    def stripe_billing_enabled(self) -> bool:
        return bool(self.stripe_secret_key and self.stripe_webhook_secret and self.stripe_price_lookup)

    # Model validators run in definition order:
    # 1. apply_env_overrides - merges env-var CSV overrides
    # 2. validate_redis_consistency - injects password into both redis_url and redis_cache_url
    # 3. default_redis_cache_url - fills redis_cache_url from redis_url (after password injection)
    # 4. validate_production_security - enforces production invariants
    # Adding new validators? Place them before validate_production_security
    # so their effects are visible to the security checks.
    MAX_SYMBOLS: ClassVar[int] = 200

    @model_validator(mode="after")
    def apply_env_overrides(self) -> Settings:
        if self.pipeline_default_symbols_csv:
            import re
            parsed = [s.strip().upper() for s in self.pipeline_default_symbols_csv.split(",") if s.strip()]
            validated: list[str] = []
            for s in parsed:
                if re.match(r'^[\^A-Z][A-Z0-9./^-]{0,15}$', s):
                    validated.append(s)
                else:
                    logger.warning("config.invalid_symbol_skipped", symbol=s)
            if validated:
                self.pipeline_default_symbols = validated
        if len(self.pipeline_default_symbols) > self.MAX_SYMBOLS:
            logger.warning(
                "config.pipeline_symbols_capped",
                original=len(self.pipeline_default_symbols),
                max=self.MAX_SYMBOLS,
            )
            self.pipeline_default_symbols = self.pipeline_default_symbols[:self.MAX_SYMBOLS]
        if not self.pipeline_default_symbols:
            raise ValueError("pipeline_default_symbols must contain at least one symbol.")
        return self

    @model_validator(mode="after")
    def validate_redis_consistency(self) -> Settings:
        if self.redis_password:
            import urllib.parse
            from urllib.parse import urlparse, urlunparse

            for attr in ("redis_url", "redis_cache_url", "celery_result_backend_url"):
                url = getattr(self, attr, None)
                if not url:
                    continue
                parsed = urlparse(url)
                encoded_pw = urllib.parse.quote(self.redis_password, safe='')
                host_part = parsed.hostname or "localhost"
                if parsed.port:
                    host_part = f"{host_part}:{parsed.port}"
                username = parsed.username or ""
                new_netloc = f"{username}:{encoded_pw}@{host_part}"
                setattr(self, attr, urlunparse(parsed._replace(netloc=new_netloc)))
        return self

    @model_validator(mode="after")
    def default_redis_cache_url(self) -> Settings:
        if not self.redis_cache_url:
            self.redis_cache_url = self.redis_url
        if not self.celery_result_backend_url:
            self.celery_result_backend_url = self.redis_url
        return self

    @model_validator(mode="after")
    def _validate_cors_no_wildcard_with_credentials(self) -> Settings:
        """Reject wildcard CORS origin in production/staging and warn elsewhere.

        The CORS spec forbids Access-Control-Allow-Origin: * when
        Access-Control-Allow-Credentials: true - browsers silently ignore
        the response.  Since the API sets allow_credentials=True for Clerk
        auth cookies, a wildcard origin will break authentication.
        """
        if "*" in self.web_cors_origins:
            if self.app_env in {"production", "staging"}:
                raise ValueError(
                    "WEB_CORS_ORIGINS contains '*' (wildcard) which is incompatible "
                    "with allow_credentials=True (required for auth cookies). "
                    "Browsers reject credentialed responses with wildcard origin "
                    "per the CORS spec. List explicit origins instead."
                )
            logger.warning(
                "config.cors_wildcard_with_credentials",
                msg=(
                    "WEB_CORS_ORIGINS contains '*'. This is incompatible with "
                    "allow_credentials=True and will break browser auth in production. "
                    "Set explicit origins instead."
                ),
            )
        return self

    @model_validator(mode="after")
    def _validate_timeout_minimums(self) -> Settings:
        _MIN_TIMEOUT = 240
        if self.scan_timeout_seconds < _MIN_TIMEOUT:
            logger.warning(
                "config.scan_timeout_too_low",
                configured=self.scan_timeout_seconds,
                minimum=_MIN_TIMEOUT,
                msg=(
                    f"scan_timeout_seconds ({self.scan_timeout_seconds}) is below the "
                    f"recommended minimum of {_MIN_TIMEOUT}s (2\u00d7 the 120s candidate timeout). "
                    "Individual candidates may not have enough time to complete."
                ),
            )
        if self.sweep_timeout_seconds < _MIN_TIMEOUT:
            logger.warning(
                "config.sweep_timeout_too_low",
                configured=self.sweep_timeout_seconds,
                minimum=_MIN_TIMEOUT,
                msg=(
                    f"sweep_timeout_seconds ({self.sweep_timeout_seconds}) is below the "
                    f"recommended minimum of {_MIN_TIMEOUT}s (2\u00d7 the 120s candidate timeout). "
                    "Individual candidates may not have enough time to complete."
                ),
            )
        return self

    @model_validator(mode="after")
    def _validate_sweep_score_weights(self) -> Settings:
        total = (
            self.sweep_score_win_rate_weight
            + self.sweep_score_roi_weight
            + self.sweep_score_sharpe_weight
            + self.sweep_score_drawdown_weight
        )
        if abs(total - 1.0) > 0.01:
            raise ValueError(
                f"sweep_score weights must sum to approximately 1.0 (within 0.01 tolerance), got {total}"
            )
        return self

    @model_validator(mode="after")
    def _validate_aws_credentials_pair(self) -> Settings:
        has_key = bool(self.aws_access_key_id)
        has_secret = bool(self.aws_secret_access_key)
        if has_key != has_secret:
            raise ValueError("aws_access_key_id and aws_secret_access_key must both be set or both be empty.")
        return self

    @model_validator(mode="after")
    def _validate_concurrency_tiers(self) -> Settings:
        if self.max_concurrent_analyses_premium < self.max_concurrent_analyses_default:
            raise ValueError(
                f"max_concurrent_analyses_premium ({self.max_concurrent_analyses_premium}) "
                f"must be >= max_concurrent_analyses_default ({self.max_concurrent_analyses_default})"
            )
        return self

    @model_validator(mode="after")
    def validate_production_security(self) -> Settings:
        _salt_is_placeholder = "default" in self.ip_hash_salt.lower() or "change" in self.ip_hash_salt.lower()
        if _salt_is_placeholder:
            if self.app_env in {"production", "staging"}:
                raise ValueError(
                    "IP_HASH_SALT contains a placeholder value. "
                    "Production/staging environments must use a unique IP_HASH_SALT."
                )
            import secrets
            self.ip_hash_salt = secrets.token_urlsafe(32)
            logger.warning(
                "config.ip_hash_salt_auto_generated",
                msg="IP_HASH_SALT was a placeholder; generated a random per-process salt for development. "
                    "Set IP_HASH_SALT explicitly for consistent IP hashing across restarts.",
            )
        if self.app_env in {"production", "staging"}:
            if not self.clerk_issuer:
                raise ValueError("Production-like environments require CLERK_ISSUER for JWT issuer verification.")
            if not (self.clerk_jwt_key or self.clerk_jwks_url):
                raise ValueError("Production-like environments require CLERK_JWT_KEY or CLERK_JWKS_URL for JWT signature verification.")
            if not self.clerk_secret_key:
                raise ValueError("Production-like environments require CLERK_SECRET_KEY for webhook verification.")
            if not self.log_json:
                raise ValueError("Production-like environments must enable structured JSON logging.")
            if "*" in self.api_allowed_hosts:
                raise ValueError("Production-like environments must not allow wildcard API hosts.")
            if "*" in self.web_cors_origins:
                raise ValueError("Production-like environments must not allow wildcard CORS origins.")
            if "default" in self.ip_hash_salt.lower() or "change" in self.ip_hash_salt.lower():
                raise ValueError("Production-like environments must use a custom IP_HASH_SALT.")
            if not self.metrics_token or not self.metrics_token.strip():
                raise ValueError("Production-like environments require METRICS_TOKEN to be set and non-blank.")
            if not self.admin_token or not self.admin_token.strip():
                raise ValueError(
                    "Production-like environments require ADMIN_TOKEN for /admin endpoints. "
                    "Without it, the DLQ endpoint falls back to METRICS_TOKEN."
                )
            redis_urls = [self.redis_url]
            if self.redis_cache_url:
                redis_urls.append(self.redis_cache_url)
            for _url in redis_urls:
                if _url.startswith("redis://") and "localhost" not in _url and "127.0.0.1" not in _url:
                    logger.warning(
                        "config.redis_unencrypted_production",
                        url=_url.split("@")[-1] if "@" in _url else _url.split("//")[-1],
                        msg="Redis URL uses unencrypted redis:// scheme in production. "
                            "Consider using rediss:// (TLS) for encrypted connections.",
                    )
            if len(self.admin_token) < 16:
                raise ValueError("admin_token must be at least 16 characters")
            if not self.redis_password:
                raise ValueError("Production-like environments require a non-empty REDIS_PASSWORD.")
            _redis_urls = [self.redis_url, self.redis_cache_url or self.redis_url]
            if any(u.startswith("redis://") for u in _redis_urls):
                logger.warning(
                    "config.redis_tls_not_configured",
                    msg="One or more Redis URLs use unencrypted redis:// scheme. "
                        "Consider using rediss:// for TLS encryption in production.",
                )
            if "sslmode" not in self.database_url and "localhost" not in self.database_url and "127.0.0.1" not in self.database_url:
                logger.warning(
                    "config.postgres_ssl_not_configured",
                    msg="DATABASE_URL does not include sslmode parameter. "
                        "Production databases should use ?sslmode=require or ?sslmode=verify-full.",
                )
            if not self.clerk_audience:
                raise ValueError("Production-like environments require CLERK_AUDIENCE for JWT audience verification.")
            if not self.clerk_authorized_parties:
                raise ValueError("Production-like environments require at least one CLERK_AUTHORIZED_PARTIES entry.")
            if self.clerk_authorized_parties_raw == "http://localhost:3000":
                raise ValueError(
                    "CLERK_AUTHORIZED_PARTIES is using the default value. "
                    "Set the actual authorized party URLs for production."
                )
            _normalized_cors = {o.strip().lower().rstrip("/") for o in self.web_cors_origins}
            _azp_not_in_cors = [
                p for p in self.clerk_authorized_parties
                if p.strip().lower().rstrip("/") not in _normalized_cors
            ]
            if _azp_not_in_cors:
                logger.warning(
                    "config.azp_cors_divergence",
                    azp_not_in_cors=_azp_not_in_cors,
                    msg=(
                        "CLERK_AUTHORIZED_PARTIES entries not present in WEB_CORS_ORIGINS. "
                        "Cookie-based auth checks the Origin header against CORS origins, "
                        "while JWT azp is checked against authorized_parties. If these lists "
                        "diverge, cookie auth may fail for valid tokens or vice versa."
                    ),
                )
            if not self.massive_base_url.startswith("https://"):
                raise ValueError("MASSIVE_BASE_URL must use HTTPS in production.")
            if "backtestforecast:backtestforecast" in self.database_url:
                raise ValueError(
                    "DATABASE_URL contains the default password 'backtestforecast:backtestforecast'. "
                    "Production-like environments require a strong, unique database password."
                )
            if not self.rate_limit_fail_closed:
                raise ValueError(
                    "Production-like environments require RATE_LIMIT_FAIL_CLOSED=true "
                    "to enforce rate limits even when Redis is unavailable."
                )
            if self.feature_billing_enabled:
                _stripe_fields = [
                    ("stripe_secret_key", self.stripe_secret_key),
                    ("stripe_webhook_secret", self.stripe_webhook_secret),
                    ("stripe_pro_monthly_price_id", self.stripe_pro_monthly_price_id),
                    ("stripe_pro_yearly_price_id", self.stripe_pro_yearly_price_id),
                    ("stripe_premium_monthly_price_id", self.stripe_premium_monthly_price_id),
                    ("stripe_premium_yearly_price_id", self.stripe_premium_yearly_price_id),
                ]
                _missing = [name for name, val in _stripe_fields if not val]
                if _missing:
                    raise ValueError(
                        f"feature_billing_enabled is True but the following Stripe env vars "
                        f"are not set: {', '.join(_missing)}"
                    )

            if self.s3_endpoint_url and not self.s3_endpoint_url.startswith("https://"):
                raise ValueError("s3_endpoint_url must use HTTPS in production.")
            if self.feature_exports_enabled and self.s3_bucket:
                _s3_fields = [
                    ("s3_region", self.s3_region),
                ]
                _s3_missing = [name for name, val in _s3_fields if not val]
                if _s3_missing:
                    raise ValueError(
                        f"S3 export storage is configured (s3_bucket={self.s3_bucket!r}) but "
                        f"the following env vars are not set: {', '.join(_s3_missing)}"
                    )
            if self.app_public_url and not self.app_public_url.startswith("https://"):
                raise ValueError(
                    f"APP_PUBLIC_URL must use HTTPS in production/staging, "
                    f"got: {self.app_public_url!r}"
                )

            import re as _re
            _sslmode_match = _re.search(r"sslmode=(\w+)", self.database_url)
            if not _sslmode_match or _sslmode_match.group(1) not in (
                "require", "verify-ca", "verify-full",
            ):
                raise ValueError(
                    "Production-like environments require sslmode=require, "
                    "sslmode=verify-ca, or sslmode=verify-full in DATABASE_URL "
                    "to encrypt Postgres traffic in transit."
                )
            _data_features = (
                self.feature_backtests_enabled
                or self.feature_scanner_enabled
                or self.feature_sweeps_enabled
                or self.feature_analysis_enabled
            )
            if _data_features and not self.massive_api_key:
                raise ValueError(
                    "MASSIVE_API_KEY is required when any data-fetching feature "
                    "(backtests, scanner, sweeps, analysis) is enabled in production."
                )

            for _url_name, _url_val in [
                ("redis_url", self.redis_url),
                ("redis_cache_url", self.redis_cache_url),
            ]:
                if _url_val and not _url_val.startswith("rediss://"):
                    logger.warning(
                        "config.redis_tls_warning",
                        msg=(
                            f"{_url_name} uses unencrypted redis:// in production. "
                            f"Consider using rediss:// for TLS encryption."
                        ),
                    )

        _data_features = (
            self.feature_backtests_enabled
            or self.feature_scanner_enabled
            or self.feature_sweeps_enabled
            or self.feature_analysis_enabled
        )
        if _data_features and not self.massive_api_key and self.app_env not in ("production", "staging"):
            logger.warning(
                "config.massive_api_key_missing",
                msg="MASSIVE_API_KEY is not set. Data-fetching features will fail at runtime.",
            )

        return self


_settings_cache: Settings | None = None
_settings_lock = threading.RLock()
_invalidation_callbacks: list[Callable[[], None]] = []


_MAX_INVALIDATION_CALLBACKS = 100


def register_invalidation_callback(callback: Callable[[], None]) -> None:
    with _settings_lock:
        if len(_invalidation_callbacks) >= _MAX_INVALIDATION_CALLBACKS:
            logger.warning(
                "config.invalidation_callback_limit_reached",
                max=_MAX_INVALIDATION_CALLBACKS,
                msg="Ignoring new callback registration; limit reached.",
            )
            return
        _invalidation_callbacks.append(callback)


def get_settings() -> Settings:
    """Return the application settings singleton.

    Unlike ``@lru_cache``, this cache can be explicitly invalidated via
    ``invalidate_settings()`` so that rotated secrets or updated environment
    variables take effect without a full process restart.
    """
    global _settings_cache
    if _settings_cache is not None:
        return _settings_cache
    with _settings_lock:
        if _settings_cache is not None:
            return _settings_cache
        _settings_cache = Settings()
        return _settings_cache


def invalidate_settings() -> None:
    """Clear cached settings so the next ``get_settings()`` creates a fresh instance.

    This reload is process-local and only affects call sites that resolve settings
    dynamically after invalidation. It does **not** rebuild already-instantiated
    application surfaces such as FastAPI metadata, OpenAPI/docs toggles, Celery beat
    schedules, or other startup-only wiring; those still require a process restart.

    Called during graceful reload or secret rotation. Currently invoked only
    via the /admin/reload endpoint (when enabled) or programmatically in tests.

    Callbacks are invoked outside the lock to avoid deadlocks if a callback
    calls ``get_settings()`` or ``register_invalidation_callback()``.
    """
    global _settings_cache
    try:
        from backtestforecast.observability.metrics import SETTINGS_INVALIDATION_TOTAL
        SETTINGS_INVALIDATION_TOTAL.inc()
    except Exception:
        pass
    with _settings_lock:
        _settings_cache = None
        callbacks = list(_invalidation_callbacks)
    for cb in callbacks:
        try:
            cb()
        except Exception:
            logger.exception("settings_invalidation_callback_failed", callback=repr(cb))
