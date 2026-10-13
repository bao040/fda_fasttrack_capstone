"""Download the openFDA NDC Directory bulk file, skipping re-download if unchanged.

Source: https://download.open.fda.gov/drug/ndc/drug-ndc-0001-of-0001.json.zip
Lands:
  landed/drug_ndc/raw.zip    -- as downloaded
  landed/drug_ndc/raw.json   -- extracted + sampled, ready for the Spark cleanup step
  landed/drug_ndc/.meta.json -- caches the remote Last-Modified header

Set NDC_SAMPLE_PERCENT (1-100, default 100) to only keep a subset of records —
useful on weaker machines, since the full file is ~245MB/116k records once
extracted. Sampling streams the zip entry with ijson so memory use stays low
regardless of percentage (never loads the full record list into memory).
"""
import json
import os
import zipfile
from pathlib import Path
from typing import Optional

import ijson
import requests

SOURCE_URL = "https://download.open.fda.gov/drug/ndc/drug-ndc-0001-of-0001.json.zip"
SAMPLE_PERCENT = float(os.environ.get("NDC_SAMPLE_PERCENT", "100"))

# Inside containers, docker-compose mounts the shared data folder at /opt/landed
# for both the airflow and spark services; fall back to a repo-relative path
# for running this script directly on the host.
_CONTAINER_LANDED_ROOT = Path("/opt/landed")
LANDED_ROOT = (
    _CONTAINER_LANDED_ROOT
    if _CONTAINER_LANDED_ROOT.is_dir()
    else Path(__file__).resolve().parents[2] / "landed"
)
LANDED_DIR = LANDED_ROOT / "drug_ndc"
ZIP_PATH = LANDED_DIR / "raw.zip"
JSON_PATH = LANDED_DIR / "raw.json"
META_PATH = LANDED_DIR / ".meta.json"


def _cached_last_modified() -> Optional[str]:
    if not META_PATH.exists():
        return None
    return json.loads(META_PATH.read_text()).get("last_modified")


def _save_last_modified(value: str) -> None:
    META_PATH.write_text(json.dumps({"last_modified": value}))


def _remote_last_modified() -> str:
    response = requests.head(SOURCE_URL, allow_redirects=True, timeout=30)
    response.raise_for_status()
    return response.headers.get("Last-Modified", "")


def _download(url: str, dest: Path) -> None:
    with requests.get(url, stream=True, timeout=300) as response:
        response.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)


def _sample_step(percent: float) -> int:
    """Keep 1 out of every N records so ~percent% of the dataset is retained."""
    percent = max(0.01, min(100.0, percent))
    return max(1, round(100 / percent))


def _extract_and_sample(zip_path: Path, dest_json: Path, percent: float) -> int:
    step = _sample_step(percent)
    with zipfile.ZipFile(zip_path) as zf:
        names = [n for n in zf.namelist() if n.endswith(".json")]
        if not names:
            raise RuntimeError(f"No .json file found inside {zip_path}")

        kept = 0
        with zf.open(names[0]) as src, open(dest_json, "w", encoding="utf-8") as out:
            out.write('{"results": [')
            for i, record in enumerate(ijson.items(src, "results.item")):
                if i % step != 0:
                    continue
                if kept > 0:
                    out.write(",")
                json.dump(record, out)
                kept += 1
            out.write("]}")
    return kept


def main() -> None:
    LANDED_DIR.mkdir(parents=True, exist_ok=True)

    remote_last_modified = _remote_last_modified()
    if remote_last_modified and remote_last_modified == _cached_last_modified() and JSON_PATH.exists():
        print(f"Already up to date (Last-Modified: {remote_last_modified}); skipping download.")
        return

    print(f"Downloading {SOURCE_URL} ...")
    _download(SOURCE_URL, ZIP_PATH)

    print(f"Extracting + sampling ({SAMPLE_PERCENT}%) from zip ...")
    kept = _extract_and_sample(ZIP_PATH, JSON_PATH, SAMPLE_PERCENT)
    print(f"Kept {kept} records")

    if remote_last_modified:
        _save_last_modified(remote_last_modified)

    print(f"Landed: {JSON_PATH}")


if __name__ == "__main__":
    main()