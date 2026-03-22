from backtestforecast import __version__

API_SERVICE_NAME = "backtestforecast-api"
API_META_PATH = "/meta"
API_HEALTH_LIVE_PATH = "/health/live"
API_HEALTH_READY_PATH = "/health/ready"
PROMETHEUS_TEXT_FORMAT_VERSION = "0.0.4"
ENGINE_VERSION_CHOICES = ("options-multileg-v1", "options-multileg-v2")
DEFAULT_ENGINE_VERSION = ENGINE_VERSION_CHOICES[-1]
RANKING_VERSION_CHOICES = ("scanner-ranking-v1", "scanner-ranking-v2")
DEFAULT_RANKING_VERSION = RANKING_VERSION_CHOICES[0]


def get_public_version() -> str:
    return __version__
