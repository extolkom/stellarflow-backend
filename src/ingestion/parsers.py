from typing import Any
from utils.signature import validate_signature

def parse_asset(asset_id: str, transaction_hash: str):
    validate_signature(asset_id.strip())
    process_transaction(transaction_hash.strip())

def process_transaction(tx_hash: str):
    pass