from __future__ import annotations

import hashlib
import time
from pathlib import Path

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

HEADERS = {"User-Agent": "LamapPDComParser/0.1"}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=30))
def download_pdf(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    time.sleep(1.0)
    with httpx.Client(headers=HEADERS, timeout=120.0, follow_redirects=True) as c:
        with c.stream("GET", url) as resp:
            resp.raise_for_status()
            with dest.open("wb") as f:
                for chunk in resp.iter_bytes(chunk_size=1 << 20):
                    f.write(chunk)
    return dest
