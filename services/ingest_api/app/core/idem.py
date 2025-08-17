import hashlib, json
from typing import Any

def canonical_request_hash(endpoint: str, body: dict[str, Any]) -> str:
    # Stable JSON: sorted keys + no whitespace
    stable = json.dumps({"endpoint": endpoint, "body": body}, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(stable.encode("utf-8")).hexdigest()