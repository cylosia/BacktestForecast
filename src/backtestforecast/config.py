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
    redis_url: str = Field(default="redis://localhost:6379/0", repr=False)
    redis_password: str | None = Field(default=None, repr=False)
    # Separate Redis URL for non-broker usage (rate limiting, SSE pub/sub, caching).
    # Defaults to redis_url when not set.  In production, point this at a dedicated
    # Redis instance or database number to isolate cache/SSE traffic from Celery broker traffic.
    redis_cache_url: str | None = Field(default=None, repr=False)

    web_cors_origins_raw: str = "http://localhost:3000"
    api_allowed_hosts_raw: str = "localhost,127.0.0.1"
    request_max_body_bytes: int = 1_048_576
    request_timeout_seconds: int = 60

    audit_cleanup_enabled: bool = True

    clerk_secret_key: str | None = Field(default=None, repr=False)
    clerk_issuer: str | None = None
    clerk_audience: str | None = None
    clerk_jwks_url: str | None = None
    clerk_jwt_key: str | None = Field(default=None, repr=False)
    clerk_jwks_fetch_timeout: float = 10.0
    jwt_leeway_seconds: int = 5
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
    # 7 days. There is currently no staleness visibility: cached data is
    # served until TTL expires with no mechanism to detect or report when
    # the upstream source has fresher data available.
    option_cache_ttl_seconds: int = 604_800
    prefetch_max_workers: int = Field(default=10, ge=1, le=32)

    # Nightly pipeline — override via PIPELINE_DEFAULT_SYMBOLS_CSV env var
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
    db_pool_recycle: int = 1800
    db_pool_timeout: int = Field(default=10, ge=1, le=120)

    trusted_proxy_cidrs: str = "127.0.0.0/8"

    rate_limit_prefix: str = "bff:rate-limit"
    rate_limit_fail_closed: bool = True
    rate_limit_memory_max_keys: int = 10_000
    backtest_create_rate_limit: int = 10
    backtest_read_rate_limit: int = 60
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
    analysis_rate_limit_window_seconds: int = 3600
    forecast_rate_limit: int = 6
    daily_picks_rate_limit: int = 30
    sse_rate_limit: int = 30
    sse_redis_max_connections: int = 50
    sse_redis_socket_timeout: float = 10.0
    sse_redis_connect_timeout: float = 5.0
    rate_limit_window_seconds: int = 60

    pipeline_max_workers: int = Field(default=20, ge=1, le=64)

    scan_timeout_seconds: int = 540
    sweep_timeout_seconds: int = Field(default=3600, ge=1)
    sweep_genetic_timeout_seconds: int = Field(default=3600, ge=1)
    max_concurrent_sweeps: int = Field(default=10, ge=1, le=100)

    sweep_score_win_rate_weight: float = 0.25
    sweep_score_roi_weight: float = 0.35
    sweep_score_sharpe_weight: float = 0.20
    sweep_score_sharpe_multiplier: float = 2.0
    sweep_score_drawdown_weight: float = 0.20

    max_concurrent_analyses_default: int = Field(default=3, ge=1, le=20)
    max_concurrent_analyses_premium: int = Field(default=5, ge=1, le=20)

    forecast_max_analogs: int = Field(default=20, ge=1)

    risk_free_rate: float = 0.045

    max_backtest_window_days: int = 1_825
    max_scanner_window_days: int = 730

    s3_bucket: str | None = None
    s3_region: str | None = None
    s3_endpoint_url: str | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = Field(default=None, repr=False)

    # Runtime feature flags — toggle via env vars without code deployment.
    # FIXME(#100): Extend to support percentage-based rollouts and
    # user-segment targeting for gradual feature releases.
    #
    # Current boolean flags are all-or-nothing. For safe rollouts of risky
    # changes, we need:
    # 1. Percentage rollout: e.g., `feature_sweeps_rollout_pct: int = 100`
    #    where 0-100 controls what fraction of users see the feature. Use
    #    a deterministic hash of `user_id` so the same user always gets a
    #    consistent experience: `hash(user_id) % 100 < rollout_pct`.
    # 2. User-segment targeting: allow flags like
    #    `feature_sweeps_segments: str = "pro,premium"` to restrict a
    #    feature to specific plan tiers during beta.
    # 3. Override list: `feature_sweeps_allow_user_ids: str = ""` for
    #    individual opt-in during internal testing.
    # 4. Store rollout config in Redis (not just env vars) so it can be
    #    changed without redeployment. The Settings singleton would read
    #    Redis overrides and merge them with env-var defaults.
    # 5. Emit structured logs when a user hits a feature gate so we can
    #    monitor adoption and error rates per cohort.
    feature_backtests_enabled: bool = True
    feature_scanner_enabled: bool = True
    feature_exports_enabled: bool = True
    feature_forecasts_enabled: bool = True
    feature_analysis_enabled: bool = True
    feature_daily_picks_enabled: bool = True
    feature_billing_enabled: bool = True
    feature_sweeps_enabled: bool = True

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
        "request_max_body_bytes", "max_backtest_window_days", "max_scanner_window_days",
        "rate_limit_window_seconds", "db_pool_size", "db_pool_recycle",
        "analysis_rate_limit_window_seconds",
        "backtest_create_rate_limit", "backtest_read_rate_limit",
        "scan_create_rate_limit", "scan_read_rate_limit",
        "sweep_create_rate_limit", "sweep_read_rate_limit",
        "export_create_rate_limit", "export_read_rate_limit",
        "billing_create_rate_limit",
        "template_mutate_rate_limit",
        "analysis_create_rate_limit", "analysis_read_rate_limit",
        "forecast_rate_limit", "daily_picks_rate_limit",
        "rate_limit_memory_max_keys",
        "sse_rate_limit", "sse_redis_max_connections",
        "scan_timeout_seconds",
        "forecast_max_analogs",
        "option_cache_ttl_seconds",
    )
    @classmethod
    def validate_positive_ints(cls, value: int) -> int:
        v = int(value)
        if v < 1:
            raise ValueError(f"Value must be >= 1, got {v}")
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

    @field_validator("massive_retry_backoff_seconds")
    @classmethod
    def validate_retry_backoff(cls, value: float) -> float:
        v = float(value)
        if v <= 0.0:
            raise ValueError(f"massive_retry_backoff_seconds must be > 0, got {v}")
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
    # 1. apply_env_overrides — merges env-var CSV overrides
    # 2. validate_redis_consistency — injects password into both redis_url and redis_cache_url
    # 3. default_redis_cache_url — fills redis_cache_url from redis_url (after password injection)
    # 4. validate_production_security — enforces production invariants
    # Adding new validators? Place them before validate_production_security
    # so their effects are visible to the security checks.
    MAX_SYMBOLS: ClassVar[int] = 200

    @model_validator(mode="after")
    def apply_env_overrides(self) -> "Settings":
        if self.pipeline_default_symbols_csv:
            import re
            parsed = [s.strip() for s in self.pipeline_default_symbols_csv.split(",") if s.strip()]
            validated: list[str] = []
            for s in parsed:
                if re.match(r'^[A-Z][A-Z0-9./^-]{0,15}$', s):
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
    def validate_redis_consistency(self) -> "Settings":
        if self.redis_password:
            import urllib.parse
            from urllib.parse import urlparse, urlunparse

            for attr in ("redis_url", "redis_cache_url"):
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
    def default_redis_cache_url(self) -> "Settings":
        if not self.redis_cache_url:
            self.redis_cache_url = self.redis_url
        return self

    @model_validator(mode="after")
    def _validate_sweep_score_weights(self) -> "Settings":
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
    def _validate_aws_credentials_pair(self) -> "Settings":
        has_key = bool(self.aws_access_key_id)
        has_secret = bool(self.aws_secret_access_key)
        if has_key != has_secret:
            raise ValueError("aws_access_key_id and aws_secret_access_key must both be set or both be empty.")
        return self

    @model_validator(mode="after")
    def validate_production_security(self) -> "Settings":
        if "default" in self.ip_hash_salt.lower() or "change" in self.ip_hash_salt.lower():
            if self.app_env in {"staging"}:
                raise ValueError(
                    "IP_HASH_SALT contains a placeholder value. "
                    "Staging environments must use a unique IP_HASH_SALT."
                )
            if self.app_env not in {"production", "staging"}:
                logger.warning(
                    "config.production_warning",
                    msg="IP_HASH_SALT appears to be a placeholder; set a unique secret in production.",
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
            if self.admin_token and len(self.admin_token) < 16:
                raise ValueError("admin_token must be at least 16 characters")
            if not self.redis_password:
                raise ValueError("Production-like environments require a non-empty REDIS_PASSWORD.")
            if not self.clerk_audience:
                raise ValueError("Production-like environments require CLERK_AUDIENCE for JWT audience verification.")
            if not self.clerk_authorized_parties:
                raise ValueError("Production-like environments require at least one CLERK_AUTHORIZED_PARTIES entry.")
            if self.clerk_authorized_parties_raw == "http://localhost:3000":
                raise ValueError(
                    "CLERK_AUTHORIZED_PARTIES is using the default value. "
                    "Set the actual authorized party URLs for production."
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
            import warnings
            warnings.warn(
                "MASSIVE_API_KEY is not set. Data-fetching features will fail at runtime.",
                stacklevel=2,
            )

        return self


_settings_cache: Settings | None = None
_settings_lock = threading.Lock()
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

    Called during graceful reload or secret rotation.  Currently invoked only
    via the /admin/reload endpoint (when enabled) or programmatically in tests.

    Callbacks are invoked outside the lock to avoid deadlocks if a callback
    calls ``get_settings()`` or ``register_invalidation_callback()``.
    """
    global _settings_cache
    with _settings_lock:
        _settings_cache = None
        callbacks = list(_invalidation_callbacks)
    for cb in callbacks:
        try:
            cb()
        except Exception:
            logger.exception("settings_invalidation_callback_failed", callback=repr(cb))
