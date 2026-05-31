import logging

logger = logging.getLogger(__name__)

def validate_asset_pair(asset_code: str):
    ASSET_MAP = {
        "USD": "US Dollar",
        "EUR": "Euro",
        "GBP": "British Pound"
    }
    try:
        result = ASSET_MAP[asset_code]
        return result
    except Exception:
        logger.warning(f"Unmapped asset code encountered: {asset_code}")