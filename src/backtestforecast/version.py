from backtestforecast import __version__

API_SERVICE_NAME = "backtestforecast-api"
API_META_PATH = "/meta"
API_HEALTH_LIVE_PATH = "/health/live"
API_HEALTH_READY_PATH = "/health/ready"


def get_public_version() -> str:
    return __version__
