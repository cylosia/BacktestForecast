from __future__ import annotations

from functools import lru_cache

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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

    database_url: str = "postgresql+psycopg://backtestforecast:backtestforecast@localhost:5432/backtestforecast"
    redis_url: str = "redis://localhost:6379/0"
    redis_password: str | None = None

    web_cors_origins_raw: str = "http://localhost:3000"
    api_allowed_hosts_raw: str = "localhost,127.0.0.1"
    request_max_body_bytes: int = 1_048_576

    clerk_secret_key: str | None = None
    clerk_issuer: str | None = None
    clerk_audience: str | None = None
    clerk_jwks_url: str | None = None
    clerk_jwt_key: str | None = None
    clerk_authorized_parties_raw: str = Field(default="http://localhost:3000")

    stripe_secret_key: str | None = None
    stripe_webhook_secret: str | None = None
    stripe_pro_monthly_price_id: str | None = None
    stripe_pro_yearly_price_id: str | None = None
    stripe_premium_monthly_price_id: str | None = None
    stripe_premium_yearly_price_id: str | None = None

    massive_api_key: str | None = None
    massive_base_url: str = "https://api.massive.com"
    massive_timeout_seconds: float = 30.0
    massive_max_retries: int = 2
    massive_retry_backoff_seconds: float = 0.5
    earnings_api_key: str | None = None

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

    metrics_token: str | None = None

    ip_hash_salt: str = Field(default="backtestforecast-default-ip-salt-change-me")

    db_pool_size: int = 5
    db_pool_max_overflow: int = Field(default=10, ge=0)
    db_pool_recycle: int = 1800

    trusted_proxy_cidrs: str = "127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16"

    rate_limit_prefix: str = "bff:rate-limit"
    rate_limit_fail_closed: bool = True
    rate_limit_memory_max_keys: int = 10_000
    backtest_create_rate_limit: int = 10
    scan_create_rate_limit: int = 6
    export_create_rate_limit: int = 20
    billing_create_rate_limit: int = 10
    template_mutate_rate_limit: int = 20
    analysis_create_rate_limit: int = 10
    analysis_rate_limit_window_seconds: int = 3600
    forecast_rate_limit: int = 6
    daily_picks_rate_limit: int = 30
    sse_rate_limit: int = 30
    sse_redis_max_connections: int = 50
    sse_redis_socket_timeout: float = 10.0
    sse_redis_connect_timeout: float = 5.0
    rate_limit_window_seconds: int = 60

    pipeline_max_workers: int = Field(default=20, ge=1, le=64)

    risk_free_rate: float = 0.045

    max_backtest_window_days: int = 1_825
    max_scanner_window_days: int = 730

    s3_bucket: str | None = None
    s3_region: str | None = None
    s3_endpoint_url: str | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None

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
        "backtest_create_rate_limit", "scan_create_rate_limit",
        "export_create_rate_limit", "billing_create_rate_limit",
        "template_mutate_rate_limit", "analysis_create_rate_limit",
        "forecast_rate_limit", "daily_picks_rate_limit",
        "rate_limit_memory_max_keys",
    )
    @classmethod
    def validate_positive_ints(cls, value: int) -> int:
        return max(int(value), 1)

    @field_validator("massive_timeout_seconds")
    @classmethod
    def validate_positive_floats(cls, value: float) -> float:
        return max(float(value), 0.1)

    @field_validator("massive_max_retries")
    @classmethod
    def validate_retry_count(cls, value: int) -> int:
        return max(0, int(value))

    @field_validator("massive_retry_backoff_seconds")
    @classmethod
    def validate_retry_backoff(cls, value: float) -> float:
        return max(float(value), 0.0)

    @property
    def web_cors_origins(self) -> list[str]:
        return [origin.strip() for origin in self.web_cors_origins_raw.split(",") if origin.strip()]

    @property
    def api_allowed_hosts(self) -> list[str]:
        hosts = [host.strip() for host in self.api_allowed_hosts_raw.split(",") if host.strip()]
        return hosts or ["localhost", "127.0.0.1"]

    @property
    def clerk_authorized_parties(self) -> list[str]:
        return [party.strip() for party in self.clerk_authorized_parties_raw.split(",") if party.strip()]

    @property
    def stripe_price_lookup(self) -> dict[tuple[str, str], str]:
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

    @model_validator(mode="after")
    def apply_env_overrides(self) -> "Settings":
        if self.pipeline_default_symbols_csv:
            parsed = [s.strip() for s in self.pipeline_default_symbols_csv.split(",") if s.strip()]
            if parsed:
                self.pipeline_default_symbols = parsed
        return self

    @model_validator(mode="after")
    def validate_redis_consistency(self) -> "Settings":
        if self.redis_password and "://:@" not in self.redis_url and "@" not in self.redis_url:
            import urllib.parse
            from urllib.parse import urlparse, urlunparse

            parsed = urlparse(self.redis_url)
            if not parsed.password and parsed.hostname:
                self.redis_url = urlunparse(
                    parsed._replace(netloc=f":{urllib.parse.quote(self.redis_password, safe='')}@{parsed.hostname}" + (f":{parsed.port}" if parsed.port else ""))
                )
        return self

    @model_validator(mode="after")
    def validate_production_security(self) -> "Settings":
        if self.app_env in {"production", "staging"}:
            if not self.clerk_issuer:
                raise ValueError("Production-like environments require CLERK_ISSUER for JWT issuer verification.")
            if not (self.clerk_jwt_key or self.clerk_jwks_url):
                raise ValueError("Production-like environments require CLERK_JWT_KEY or CLERK_JWKS_URL for JWT signature verification.")
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
            if not self.redis_password:
                raise ValueError("Production-like environments require a non-empty REDIS_PASSWORD.")
            if not self.clerk_audience:
                raise ValueError("Production-like environments require CLERK_AUDIENCE for JWT audience verification.")
            if not self.clerk_authorized_parties:
                raise ValueError("Production-like environments require at least one CLERK_AUTHORIZED_PARTIES entry.")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
