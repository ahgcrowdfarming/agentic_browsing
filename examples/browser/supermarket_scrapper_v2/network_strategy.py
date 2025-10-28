# network_strategy.py
from __future__ import annotations
import os, json, random
from typing import List, Optional, Tuple, Dict

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

# Optional mapping if you pass full country names in your tasks
ISO2_MAP = {
    "France": "FR", "Germany": "DE", "Spain": "ES", "Italy": "IT",
    # extend as needed
}

def _load_proxy_pool() -> Dict[str, List[str]]:
    raw = os.environ.get("PROXY_POOL_JSON")
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        # Ensure lists
        return {k: (v if isinstance(v, list) else [v]) for k, v in data.items()}
    except Exception:
        return {}

def pick_proxy_for_country(country: str) -> Optional[str]:
    """
    Decide the proxy for this task based on env:
      - PROXY_POOL_JSON (preferred; per-country arrays)
      - PROXY_SERVER (fallback)
    Returns a single proxy URL or None.
    """
    pool = _load_proxy_pool()
    if not pool and not os.environ.get("PROXY_SERVER"):
        return None

    iso2 = ISO2_MAP.get(country, country[:2].upper())
    candidates = pool.get(iso2) or pool.get(country) or pool.get("DEFAULT") or []
    if candidates:
        return random.choice(candidates)
    return os.environ.get("PROXY_SERVER")

def user_agent() -> str:
    """Take UA from env (BROWSER_USER_AGENT) or fallback to a sane desktop UA."""
    return os.environ.get("BROWSER_USER_AGENT") or DEFAULT_UA

def base_browser_args(country: str) -> List[str]:
    """
    Build generic args that work everywhere, *without* hardcoding CI concepts.
    All behavior is driven by env so local runs remain unchanged.
    """
    args = [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--proxy-bypass-list=*",
        "--disable-blink-features=AutomationControlled",
        "--window-size=1920,1080",
        "--force-device-scale-factor=1",
    ]

    # Locale hint (optionally vary by country if you want)
    lang = os.environ.get("BROWSER_LANG")
    if not lang:
        # tiny default; you can make this a mapping per country if needed
        lang = "fr-FR" if country.lower().startswith("fr") else "en-US"
    args.append(f"--lang={lang}")

    # Optional UA and proxy (if provided)
    ua = user_agent()
    if ua:
        args.append(f"--user-agent={ua}")

    proxy = pick_proxy_for_country(country)
    if proxy:
        args.append(f"--proxy-server={proxy}")

    # Allow appending arbitrary flags from env without code changes
    extra = os.environ.get("BROWSER_EXTRA_ARGS", "")
    if extra:
        args += extra.split()

    return args
