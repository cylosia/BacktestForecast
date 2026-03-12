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

    # Nightly pipeline
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

    ip_hash_salt: str = "bff-default-ip-hash-salt-change-in-production"

    db_pool_size: int = 5
    db_pool_recycle: int = 1800

    trusted_proxy_cidrs: str = "127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16"

    rate_limit_prefix: str = "bff:rate-limit"
    rate_limit_fail_closed: bool = False
    backtest_create_rate_limit: int = 10
    scan_create_rate_limit: int = 6
    export_create_rate_limit: int = 20
    billing_create_rate_limit: int = 10
    template_mutate_rate_limit: int = 20
    analysis_create_rate_limit: int = 10
    analysis_rate_limit_window_seconds: int = 3600
    forecast_rate_limit: int = 6
    rate_limit_window_seconds: int = 60

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
        return normalized or "development"

    @field_validator("log_level")
    @classmethod
    def normalize_log_level(cls, value: str) -> str:
        normalized = value.strip().upper()
        return normalized or "INFO"

    @field_validator("request_max_body_bytes", "max_backtest_window_days", "max_scanner_window_days")
    @classmethod
    def validate_positive_ints(cls, value: int) -> int:
        return max(int(value), 1)

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
    def validate_production_security(self) -> "Settings":
        if self.app_env in {"production", "staging"}:
            if not (self.clerk_jwt_key or self.clerk_jwks_url or self.clerk_issuer):
                raise ValueError("Production-like environments require CLERK_JWT_KEY, CLERK_JWKS_URL, or CLERK_ISSUER.")
            if not self.log_json:
                raise ValueError("Production-like environments must enable structured JSON logging.")
            if "*" in self.api_allowed_hosts:
                raise ValueError("Production-like environments must not allow wildcard API hosts.")
            if "*" in self.web_cors_origins:
                raise ValueError("Production-like environments must not allow wildcard CORS origins.")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
