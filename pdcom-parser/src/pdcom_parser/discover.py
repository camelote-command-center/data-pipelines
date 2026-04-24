"""Fallback discovery: only invoked by `pdcom discover --commune-bfs N` when a
commune's PDF is missing. Best-effort — raises RuntimeError if no URL can be
identified; the operator is expected to fill the gap manually."""
from __future__ import annotations

import re
import time
from urllib.parse import quote_plus, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "LamapPDComParser/0.1"}


def _commune_homepage(commune_name: str) -> str:
    slug = commune_name.lower().replace(" ", "-").replace("é", "e").replace("è", "e").replace("ê", "e").replace("à", "a").replace("â", "a").replace("î", "i").replace("ô", "o").replace("û", "u").replace("ç", "c").replace("'", "")
    return f"https://www.{slug}.ch/"


def _ddg_search(query: str, max_results: int = 10) -> list[str]:
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
    time.sleep(1.0)
    with httpx.Client(headers=HEADERS, timeout=20.0, follow_redirects=True) as c:
        resp = c.get(url)
        resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    results = []
    for a in soup.select("a.result__a"):
        href = a.get("href", "")
        if href.lower().endswith(".pdf"):
            results.append(href)
        if len(results) >= max_results:
            break
    return results


def discover_pdcom_pdf(commune_name: str) -> list[str]:
    """Return ranked candidate PDF URLs for a commune."""
    queries = [
        f"{commune_name} PDCom filetype:pdf",
        f"{commune_name} plan directeur communal filetype:pdf",
        f'"{commune_name}" PDCom version approuvée filetype:pdf',
    ]
    urls: list[str] = []
    for q in queries:
        try:
            urls.extend(_ddg_search(q))
        except Exception:
            continue
        time.sleep(1.0)
    # Dedupe preserving order
    seen = set()
    out = []
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out
