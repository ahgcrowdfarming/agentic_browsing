import os, math, json, logging
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

class FoundryError(Exception): pass

class FoundryClient:
    def __init__(self, base_url: str, token: str, dataset_rid: str|None=None, endpoint_path: str|None=None):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.dataset_rid = dataset_rid
        self.endpoint_path = endpoint_path

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    @retry(
        retry=retry_if_exception_type(httpx.HTTPError),
        wait=wait_exponential(multiplier=1, min=1, max=20),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def post_json(self, path: str, payload: dict|list):
        url = f"{self.base_url}{path}"
        with httpx.Client(timeout=60) as client:
            r = client.post(url, headers=self._headers(), json=payload)
            if r.status_code >= 300:
                raise FoundryError(f"POST {url} -> {r.status_code}: {r.text[:500]}")
            return r.json() if r.headers.get("content-type","").startswith("application/json") else r.text

def chunk(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i+size]

def post_records(fc: FoundryClient, records: list[dict], batch_size: int = 500):
    """
    Two common patterns:
    1) Dataset append endpoint: e.g., /api/datasets/{rid}/append
       payload: {"records":[...]}
    2) Custom endpoint: fc.endpoint_path that accepts the list or an object.

    Adjust the path/payload below to your actual Foundry API.
    """
    if fc.endpoint_path:
        for part in chunk(records, batch_size):
            logging.info(f"Posting {len(part)} records to {fc.endpoint_path}")
            fc.post_json(fc.endpoint_path, part)  # or {"records": part}
    elif fc.dataset_rid:
        path = f"/api/datasets/{fc.dataset_rid}/append"
        for part in chunk(records, batch_size):
            logging.info(f"Appending {len(part)} records to dataset {fc.dataset_rid}")
            fc.post_json(path, {"records": part})
    else:
        raise FoundryError("Must set either endpoint_path or dataset_rid")
