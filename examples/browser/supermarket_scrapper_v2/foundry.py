# foundry.py
from __future__ import annotations
import os
import time
import requests
from typing import Optional
from urllib.parse import urlparse, urljoin


def _build_foundry_url(host: str, path: str) -> str:
    """
    Ensure host has a scheme and build a proper absolute URL for requests.
    Accepts host values like "crowdfarming.palantirfoundry.com" or "https://crowdfarming..."
    """
    if not host:
        raise ValueError("Foundry host is empty. Set FOUNDRY_HOST env var.")
    host = host.strip()
    # if user mistakenly passed something like 'https' or '/crowdfarming...', normalize it
    if host.startswith("http:/") and not host.startswith("http://"):
        host = host.replace("http:/", "http://", 1)
    if host.startswith("https:/") and not host.startswith("https://"):
        host = host.replace("https:/", "https://", 1)
    parsed = urlparse(host)
    if not parsed.scheme:
        host = "https://" + host.lstrip("/")
    # join and ensure no duplicate slashes
    return urljoin(host if host.endswith("/") else host + "/", path.lstrip("/"))


class FoundryUploader:
    """
    Minimal client for Foundry's append-only file upload:
      POST https://{host}/api/v1/datasets/{rid}/files:upload?filePath=...
    Env vars required:
      - FOUNDRY_HOST (e.g. foundry.mycompany.com)
      - FOUNDRY_DATASET_RID (e.g. ri.dataset.main.xxxxx)
      - FOUNDRY_TOKEN (Bearer token)
    Optional:
      - FOUNDRY_FOLDER_PREFIX (default: 'scrapes')
    """
    def __init__(
        self,
        host: Optional[str] = None,
        dataset_rid: Optional[str] = None,
        token: Optional[str] = None,
        folder_prefix: Optional[str] = None,
        timeout_seconds: int = 120,
    ):
        self.host = (host or os.environ.get("FOUNDRY_HOST", "")).strip().rstrip("/")
        self.dataset_rid = (dataset_rid or os.environ.get("FOUNDRY_DATASET_RID", "")).strip()
        self.token = (token or os.environ.get("FOUNDRY_TOKEN", "")).strip()
        self.folder_prefix = (folder_prefix or os.environ.get("FOUNDRY_FOLDER_PREFIX", "scrapes")).strip("/")
        self.timeout_seconds = timeout_seconds

        if not self.host or not self.dataset_rid or not self.token:
            raise ValueError("Missing FOUNDRY_HOST, FOUNDRY_DATASET_RID, or FOUNDRY_TOKEN")

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/octet-stream",
        }

    def unique_dataset_path(self, filename: str) -> str:
        """
        Build an append-only path: prefix/year=YYYY/month=MM/day=DD/<timestamp>_filename
        Prevents overwrites and plays nicely with partitioned views.
        """
        y = time.strftime("%Y")
        m = time.strftime("%m")
        d = time.strftime("%d")
        ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        return f"{self.folder_prefix}/year={y}/month={m}/day={d}/{ts}_{filename}"

    def upload_bytes(self, data: bytes, file_path_in_dataset: str):
        dataset_path = f"https://{self.host}/api/v1/datasets/{self.dataset_rid}/files:upload"
        url = _build_foundry_url(self.host, dataset_path)
        print(f"Uploading to Foundry URL: {url}")
        resp = requests.post(
            url,
            params={"filePath": file_path_in_dataset},
            data=data,
            headers=self._headers(),
            timeout=self.timeout_seconds,
        )
        if resp.status_code >= 300:
            raise RuntimeError(f"Foundry upload failed {resp.status_code}: {resp.text[:500]}")
        return resp
