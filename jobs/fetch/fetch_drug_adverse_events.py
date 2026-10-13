"""Fetch drug adverse event reports from openFDA, filtered by received-date range.

Set AE_START_DATE / AE_END_DATE (YYYYMMDD, default a 1-year window) to control
volume — openFDA caps skip at 25000 without search_after pagination, so keep
the date range narrow enough that total stays under that.

Set OPENFDA_API_KEY (optional, free — https://open.fda.gov/apis/authentication/)
to raise the daily quota from 1,000 to 120,000 requests; the per-minute cap
(240 req/min) applies either way, which is what REQUEST_INTERVAL_SEC respects.

Set AE_MAX_RECORDS to cap the total number of records fetched regardless of
how many actually match the date range — handy for a quick test run
(e.g. AE_MAX_RECORDS=1000). Leave unset to fetch everything in range.
"""
import json
import os
import re
import time
from pathlib import Path

import requests

BASE_URL = "https://api.fda.gov/drug/event.json"
START_DATE = os.environ.get("AE_START_DATE", "20250101")
END_DATE = os.environ.get("AE_END_DATE", "20251231")
API_KEY = os.environ.get("OPENFDA_API_KEY", "")


MAX_RECORDS = 5000

# MAX_SKIP = 25000  # openFDA hard limit without search_after
PAGE_LIMIT = 1000  # openFDA's hard cap on the limit param, per request
REQUEST_INTERVAL_SEC = 0.3  # keeps us under the 240 requests/minute cap (240/60 = 4/sec max)
MAX_RETRIES = 5

_LINK_NEXT_RE = re.compile(r'<([^>]+)>;\s*rel="next"')

_CONTAINER_LANDED_ROOT = Path("/opt/landed")
LANDED_ROOT = (
    _CONTAINER_LANDED_ROOT
    if _CONTAINER_LANDED_ROOT.is_dir()
    else Path(__file__).resolve().parents[2] / "landed"
)
LANDED_DIR = LANDED_ROOT / "drug_adverse_events"
JSON_PATH = LANDED_DIR / "raw.json"


def _fetch_page(url: str, params: dict | None) -> requests.Response:
    for attempt in range(1, MAX_RETRIES + 1):
        response = requests.get(url, params=params, timeout=60)
        if response.status_code == 429:
            wait = 2**attempt  # exponential backoff: 2s, 4s, 8s, 16s, 32s
            print(f"Rate limited (429), retrying in {wait}s (attempt {attempt}/{MAX_RETRIES})...")
            time.sleep(wait)
            continue
        response.raise_for_status()
        return response

    raise RuntimeError(f"Gave up after {MAX_RETRIES} retries (url={url})")


def main() -> None:
    LANDED_DIR.mkdir(parents=True, exist_ok=True)

    if not API_KEY:
        print("No OPENFDA_API_KEY set — limited to 1,000 requests/day for this IP.")
    if MAX_RECORDS:
        print(f"AE_MAX_RECORDS set — stopping after {MAX_RECORDS} records.")

    all_results = []

    total = None

    params = {
        "search": f"receivedate:[{START_DATE} TO {END_DATE}]",
        "limit": PAGE_LIMIT,
        "sort": "receivedate:asc"
    }

    if API_KEY:
        params["api_key"] = API_KEY

    url = BASE_URL
    use_params = params

    while True:
        response = _fetch_page(url, use_params)
        page = response.json()

        if total is None:
            total = page["meta"]["results"]["total"]
            print(f"Date range {START_DATE}-{END_DATE}: {total} total records matching")


        results = page.get("results", [])
        all_results.extend(results)

        if MAX_RECORDS and len(all_results) >= MAX_RECORDS:
            break

        link_header = response.headers.get("Link", "")
        match = _LINK_NEXT_RE.search(link_header)
        if not match:
            break

        url = match.group(1)

        if API_KEY and "api_key=" not in url:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}api_key={API_KEY}"
        use_params = None
        time.sleep(REQUEST_INTERVAL_SEC)

    if MAX_RECORDS:
        all_results = all_results[:MAX_RECORDS]

    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump({"results": all_results}, f)

    print(f"Landed {len(all_results)} records to {JSON_PATH}")


if __name__ == "__main__":
    main()

