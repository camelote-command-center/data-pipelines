#!/usr/bin/env python3
"""
SILGeneve real-estate laws parser.

Scrapes Geneva cantonal real-estate-related laws from silgeneve.ch and writes
them to bronze_ch on re-LLM.

Entry point: `python pipelines/silgeneve/parser.py`

Auth (in order of precedence):
  1. RE_LLM_DB_URL env var (preferred, used by GitHub Actions)
  2. ~/supabase-registry/supabase-projects.json re-llm entry (local dev fallback)

Conventions followed (from Camelote v2 architecture):
  - Target DB: re-LLM (znrvddgmczdqoucmykij)
  - Schema:    bronze_ch
  - Tables:    silgeneve_laws, silgeneve_articles, silgeneve_fetch_log
  - UPSERT via ON CONFLICT, never TRUNCATE
  - Content-hash change detection to avoid unnecessary writes
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import quote

import psycopg2
import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
LAWS_REGISTRY_PATH = SCRIPT_DIR / "laws_registry.json"
TARGET_PROJECT_REF = "znrvddgmczdqoucmykij"  # re-LLM
REGISTRY_PATH = Path(
    os.getenv(
        "SUPABASE_REGISTRY_PATH",
        str(Path.home() / "supabase-registry" / "supabase-projects.json"),
    )
)

URL_BASE = "https://silgeneve.ch/legis/data/"
USER_AGENT = "LAMAP-data-pipeline/silgeneve (+https://lamap.ch)"
REQUEST_TIMEOUT_S = 30
REQUEST_RETRIES = 3
REQUEST_BACKOFF_S = 5
POLITE_DELAY_S = 1.5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("silgeneve")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class LawMeta:
    rsge: str
    url_slug: str
    short_name: str
    full_title: str
    law_type: str
    domain: str
    priority: int
    notes: str | None = None


@dataclass
class ParsedLaw:
    rsge: str
    url_slug: str
    source_url: str
    full_title: str
    adopted_date: date | None
    entry_in_force: date | None
    last_modified: date | None
    content_html: str
    content_md: str
    content_hash: str
    modifications_history: list[dict[str, Any]]
    articles: list[dict[str, Any]] = field(default_factory=list)
    word_count: int = 0


# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------


def load_db_connection_string() -> str:
    """
    Resolve a psycopg2-compatible connection string for re-LLM.

    Order:
      1. RE_LLM_DB_URL env var (GitHub Actions secret)
      2. supabase-registry JSON (local dev)
    """
    env_url = os.getenv("RE_LLM_DB_URL")
    if env_url:
        return env_url

    if not REGISTRY_PATH.exists():
        raise RuntimeError(
            "RE_LLM_DB_URL is not set and supabase-registry is missing. "
            f"Tried: {REGISTRY_PATH}. In GitHub Actions, set the RE_LLM_DB_URL secret."
        )
    with REGISTRY_PATH.open(encoding="utf-8") as f:
        registry = json.load(f)

    projects = registry.get("projects", registry)
    entry = projects.get("re-llm") or projects.get("re-LLM") or projects.get("rellm")
    if not entry:
        raise KeyError(f"re-llm entry not found in registry. Keys: {list(projects.keys())}")

    conn_str = (
        entry.get("direct_connection_string")
        or entry.get("connection_string")
        or entry.get("pooler_connection_string")
    )
    if conn_str:
        return conn_str

    # Build from url + db_password (registry schema used in this repo)
    url = entry.get("url") or ""
    password = entry.get("db_password")
    ref_match = re.search(r"https?://([a-z0-9]+)\.supabase\.co", url)
    if not (password and ref_match):
        raise KeyError(
            "re-llm registry entry missing db_password or url. "
            f"Keys present: {list(entry.keys())}"
        )
    ref = ref_match.group(1)
    return (
        f"postgresql://postgres:{quote(password, safe='')}"
        f"@db.{ref}.supabase.co:5432/postgres?sslmode=require"
    )


# ---------------------------------------------------------------------------
# HTTP fetch
# ---------------------------------------------------------------------------


def fetch_law_html(url_slug: str) -> tuple[str, int]:
    url = URL_BASE + url_slug
    last_exc: Exception | None = None
    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            resp = requests.get(
                url,
                headers={"User-Agent": USER_AGENT},
                timeout=REQUEST_TIMEOUT_S,
            )
            resp.encoding = "windows-1252"
            resp.raise_for_status()
            return resp.text, resp.status_code
        except requests.RequestException as e:
            last_exc = e
            log.warning(
                "Fetch attempt %d/%d failed for %s: %s",
                attempt, REQUEST_RETRIES, url, e,
            )
            if attempt < REQUEST_RETRIES:
                time.sleep(REQUEST_BACKOFF_S * attempt)
    raise RuntimeError(f"Failed to fetch {url} after {REQUEST_RETRIES} attempts") from last_exc


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

DATE_RE = re.compile(r"\b(\d{1,2})[\./ ](\d{1,2})[\./ ](\d{2,4})\b")
FRENCH_MONTHS = {
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4, "mai": 5,
    "juin": 6, "juillet": 7, "août": 8, "aout": 8, "septembre": 9,
    "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12,
}
FRENCH_DATE_RE = re.compile(
    r"(\d{1,2})\s+(" + "|".join(FRENCH_MONTHS.keys()) + r")\s+(\d{4})",
    re.IGNORECASE,
)


def parse_french_date(text: str) -> date | None:
    text = text.strip()
    m = FRENCH_DATE_RE.search(text)
    if m:
        d = int(m.group(1))
        mo = FRENCH_MONTHS[m.group(2).lower()]
        y = int(m.group(3))
        try:
            return date(y, mo, d)
        except ValueError:
            return None
    m = DATE_RE.search(text)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if y < 100:
            y += 1900 if y > 50 else 2000
        try:
            return date(y, mo, d)
        except ValueError:
            return None
    return None


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def html_to_markdown(soup: BeautifulSoup) -> str:
    for tag in soup.find_all(["script", "style", "nav"]):
        tag.decompose()

    parts: list[str] = []
    for el in soup.body.descendants if soup.body else soup.descendants:
        if not hasattr(el, "name") or el.name is None:
            continue
        name = el.name.lower()
        if name in {"h1", "h2", "h3", "h4"}:
            level = int(name[1])
            text = el.get_text(" ", strip=True)
            if text:
                parts.append("\n" + "#" * level + " " + text + "\n")
        elif name == "p":
            text = el.get_text(" ", strip=True)
            if text:
                parts.append(text + "\n")
        elif name == "br":
            parts.append("\n")

    md = "\n".join(parts)
    md = re.sub(r"\n{3,}", "\n\n", md)
    md = re.sub(r"[ \t]+", " ", md)
    return md.strip()


def extract_articles(soup: BeautifulSoup) -> list[dict[str, Any]]:
    text = soup.get_text("\n", strip=False)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n\n", text)

    art_header_re = re.compile(
        r"^\s*Art\.\s*(\d+[A-Z]?)(?:\(\d+\))?\s+([^\n]+?)\s*$",
        re.MULTILINE,
    )
    headers = list(art_header_re.finditer(text))
    if not headers:
        return []

    chapter_re = re.compile(
        r"^\s*Chapitre\s+([IVXLC]+|\d+)\s*[-—]?\s*([^\n]+?)\s*$",
        re.MULTILINE,
    )
    section_re = re.compile(
        r"^\s*Section\s+(\d+|[IVXLC]+)\s*[-—]?\s*([^\n]+?)\s*$",
        re.MULTILINE,
    )

    articles: list[dict[str, Any]] = []
    for i, header in enumerate(headers):
        start = header.end()
        end = headers[i + 1].start() if i + 1 < len(headers) else len(text)
        body = text[start:end].strip()

        preceding = text[: header.start()]
        chapters = list(chapter_re.finditer(preceding))
        sections = list(section_re.finditer(preceding))
        chapter = None
        if chapters:
            c = chapters[-1]
            chapter = f"Chapitre {c.group(1)} — {c.group(2).strip()}"
        section = None
        if sections:
            s = sections[-1]
            section = f"Section {s.group(1)} — {s.group(2).strip()}"

        article_number = f"Art. {header.group(1)}"
        article_title = header.group(2).strip()

        articles.append({
            "article_number": article_number,
            "article_title": article_title,
            "chapter": chapter,
            "section": section,
            "content": body,
            "article_order": i + 1,
            "content_hash": sha256(f"{article_number}|{body}"),
        })

    return articles


def extract_modifications_history(soup: BeautifulSoup) -> list[dict[str, Any]]:
    history: list[dict[str, Any]] = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        has_mod_header = any(
            "modifications" in (r.get_text(" ", strip=True) or "").lower()
            for r in rows
        )
        if not has_mod_header:
            continue
        for row in rows:
            cells = [c.get_text(" ", strip=True) for c in row.find_all(["td", "th"])]
            if len(cells) < 3:
                continue
            if "modifications" in cells[0].lower():
                continue
            if cells[0].startswith("RSG") or "Date d'adoption" in " ".join(cells):
                continue
            adopted_parsed = parse_french_date(cells[1])
            in_force_parsed = parse_french_date(cells[2])
            history.append({
                "description": cells[0],
                "adopted": adopted_parsed.isoformat() if adopted_parsed else cells[1],
                "in_force": in_force_parsed.isoformat() if in_force_parsed else cells[2],
            })
        break
    return history


def extract_dates_from_header(text: str) -> tuple[date | None, date | None, date | None]:
    last_modified = None
    adopted = None
    entry_in_force = None

    m = re.search(r"Derni[èe]res modifications au\s+([^\n]+)", text, re.IGNORECASE)
    if m:
        last_modified = parse_french_date(m.group(1))

    m = re.search(r"^du\s+(\d{1,2}\s+\w+\s+\d{4})", text, re.MULTILINE | re.IGNORECASE)
    if m:
        adopted = parse_french_date(m.group(1))

    m = re.search(r"Entr[ée]e en vigueur\s*:?\s*([^\)\n]+)", text, re.IGNORECASE)
    if m:
        entry_in_force = parse_french_date(m.group(1))

    return adopted, entry_in_force, last_modified


def parse_law(meta: LawMeta, html: str) -> ParsedLaw:
    soup = BeautifulSoup(html, "html.parser")
    raw_text = soup.get_text("\n", strip=False)

    adopted, entry_in_force, last_modified = extract_dates_from_header(raw_text)
    articles = extract_articles(soup)
    modifications = extract_modifications_history(soup)
    content_md = html_to_markdown(soup)
    word_count = len(content_md.split())

    return ParsedLaw(
        rsge=meta.rsge,
        url_slug=meta.url_slug,
        source_url=URL_BASE + meta.url_slug,
        full_title=meta.full_title,
        adopted_date=adopted,
        entry_in_force=entry_in_force,
        last_modified=last_modified,
        content_html=str(soup),
        content_md=content_md,
        content_hash=sha256(content_md),
        modifications_history=modifications,
        articles=articles,
        word_count=word_count,
    )


# ---------------------------------------------------------------------------
# Database writes
# ---------------------------------------------------------------------------


UPSERT_LAW_SQL = """
INSERT INTO bronze_ch.silgeneve_laws (
  law_rsge, url_slug, source_url, short_name, full_title,
  law_type, domain, priority, notes,
  adopted_date, entry_in_force, last_modified,
  content_html, content_md, content_hash,
  modifications_history, article_count, word_count,
  fetched_at, updated_at
) VALUES (
  %(law_rsge)s, %(url_slug)s, %(source_url)s, %(short_name)s, %(full_title)s,
  %(law_type)s, %(domain)s, %(priority)s, %(notes)s,
  %(adopted_date)s, %(entry_in_force)s, %(last_modified)s,
  %(content_html)s, %(content_md)s, %(content_hash)s,
  %(modifications_history)s::jsonb, %(article_count)s, %(word_count)s,
  now(), now()
)
ON CONFLICT (law_rsge) DO UPDATE SET
  url_slug              = EXCLUDED.url_slug,
  source_url            = EXCLUDED.source_url,
  short_name            = EXCLUDED.short_name,
  full_title            = EXCLUDED.full_title,
  law_type              = EXCLUDED.law_type,
  domain                = EXCLUDED.domain,
  priority              = EXCLUDED.priority,
  notes                 = EXCLUDED.notes,
  adopted_date          = EXCLUDED.adopted_date,
  entry_in_force        = EXCLUDED.entry_in_force,
  last_modified         = EXCLUDED.last_modified,
  content_html          = EXCLUDED.content_html,
  content_md            = EXCLUDED.content_md,
  content_hash          = EXCLUDED.content_hash,
  modifications_history = EXCLUDED.modifications_history,
  article_count         = EXCLUDED.article_count,
  word_count            = EXCLUDED.word_count,
  fetched_at            = now(),
  updated_at            = now()
WHERE bronze_ch.silgeneve_laws.content_hash IS DISTINCT FROM EXCLUDED.content_hash;
"""

UPSERT_ARTICLE_SQL = """
INSERT INTO bronze_ch.silgeneve_articles (
  law_rsge, article_number, article_order, article_title,
  chapter, section, content, content_hash, fetched_at
) VALUES (
  %(law_rsge)s, %(article_number)s, %(article_order)s, %(article_title)s,
  %(chapter)s, %(section)s, %(content)s, %(content_hash)s, now()
)
ON CONFLICT (law_rsge, article_number) DO UPDATE SET
  article_order = EXCLUDED.article_order,
  article_title = EXCLUDED.article_title,
  chapter       = EXCLUDED.chapter,
  section       = EXCLUDED.section,
  content       = EXCLUDED.content,
  content_hash  = EXCLUDED.content_hash,
  fetched_at    = now()
WHERE bronze_ch.silgeneve_articles.content_hash IS DISTINCT FROM EXCLUDED.content_hash;
"""

DELETE_STALE_ARTICLES_SQL = """
DELETE FROM bronze_ch.silgeneve_articles
WHERE law_rsge = %s AND article_number != ALL(%s);
"""

INSERT_FETCH_LOG_SQL = """
INSERT INTO bronze_ch.silgeneve_fetch_log (
  run_id, law_rsge, status, http_status,
  content_changed, articles_parsed, error_message, duration_ms
) VALUES (
  %(run_id)s, %(law_rsge)s, %(status)s, %(http_status)s,
  %(content_changed)s, %(articles_parsed)s, %(error_message)s, %(duration_ms)s
);
"""

SELECT_EXISTING_HASH_SQL = """
SELECT content_hash FROM bronze_ch.silgeneve_laws WHERE law_rsge = %s;
"""


def upsert_law(conn, meta: LawMeta, parsed: ParsedLaw) -> bool:
    with conn.cursor() as cur:
        cur.execute(SELECT_EXISTING_HASH_SQL, (meta.rsge,))
        row = cur.fetchone()
        existing_hash = row[0] if row else None
        changed = existing_hash != parsed.content_hash

        cur.execute(UPSERT_LAW_SQL, {
            "law_rsge": meta.rsge,
            "url_slug": meta.url_slug,
            "source_url": parsed.source_url,
            "short_name": meta.short_name,
            "full_title": meta.full_title,
            "law_type": meta.law_type,
            "domain": meta.domain,
            "priority": meta.priority,
            "notes": meta.notes,
            "adopted_date": parsed.adopted_date,
            "entry_in_force": parsed.entry_in_force,
            "last_modified": parsed.last_modified,
            "content_html": parsed.content_html,
            "content_md": parsed.content_md,
            "content_hash": parsed.content_hash,
            "modifications_history": json.dumps(parsed.modifications_history, ensure_ascii=False),
            "article_count": len(parsed.articles),
            "word_count": parsed.word_count,
        })

        if changed or existing_hash is None:
            for art in parsed.articles:
                cur.execute(UPSERT_ARTICLE_SQL, {"law_rsge": meta.rsge, **art})
            current_numbers = [a["article_number"] for a in parsed.articles]
            if current_numbers:
                cur.execute(DELETE_STALE_ARTICLES_SQL, (meta.rsge, current_numbers))

    return changed


def log_fetch(
    conn,
    run_id: uuid.UUID,
    law_rsge: str | None,
    status: str,
    http_status: int | None = None,
    content_changed: bool | None = None,
    articles_parsed: int | None = None,
    error_message: str | None = None,
    duration_ms: int | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(INSERT_FETCH_LOG_SQL, {
            "run_id": str(run_id),
            "law_rsge": law_rsge,
            "status": status,
            "http_status": http_status,
            "content_changed": content_changed,
            "articles_parsed": articles_parsed,
            "error_message": error_message,
            "duration_ms": duration_ms,
        })


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def load_registry() -> list[LawMeta]:
    with LAWS_REGISTRY_PATH.open(encoding="utf-8") as f:
        data = json.load(f)
    return [
        LawMeta(
            rsge=entry["rsge"],
            url_slug=entry["url_slug"],
            short_name=entry.get("short_name"),
            full_title=entry["full_title"],
            law_type=entry["law_type"],
            domain=entry["domain"],
            priority=entry.get("priority", 3),
            notes=entry.get("notes"),
        )
        for entry in data["laws"]
    ]


def main() -> int:
    run_id = uuid.uuid4()
    log.info("SILGeneve parser run starting. run_id=%s", run_id)

    try:
        registry = load_registry()
    except Exception as e:
        log.exception("Failed to load laws registry: %s", e)
        return 1

    log.info("Loaded %d laws from registry.", len(registry))

    try:
        conn_str = load_db_connection_string()
    except Exception as e:
        log.exception("Failed to load DB connection string: %s", e)
        return 1

    try:
        conn = psycopg2.connect(conn_str)
        conn.autocommit = False
    except Exception as e:
        log.exception("Failed to connect to re-LLM: %s", e)
        return 1

    try:
        log_fetch(conn, run_id, None, "run_start")
        conn.commit()

        changed_count = 0
        error_count = 0
        unchanged_count = 0

        for i, meta in enumerate(registry, 1):
            log.info("[%d/%d] Fetching %s (%s)", i, len(registry), meta.rsge, meta.short_name)
            start_ms = time.time()
            try:
                html, http_status = fetch_law_html(meta.url_slug)
                parsed = parse_law(meta, html)
                changed = upsert_law(conn, meta, parsed)
                conn.commit()

                duration_ms = int((time.time() - start_ms) * 1000)
                status = "success" if changed else "unchanged"
                log_fetch(
                    conn, run_id, meta.rsge, status,
                    http_status=http_status,
                    content_changed=changed,
                    articles_parsed=len(parsed.articles),
                    duration_ms=duration_ms,
                )
                conn.commit()

                if changed:
                    changed_count += 1
                    log.info(
                        "  → %s CHANGED (%d articles, %d words, last mod: %s)",
                        meta.rsge, len(parsed.articles), parsed.word_count, parsed.last_modified,
                    )
                else:
                    unchanged_count += 1
                    log.info("  → %s unchanged (%d articles)", meta.rsge, len(parsed.articles))

            except Exception as e:
                error_count += 1
                conn.rollback()
                log.exception("Error processing %s: %s", meta.rsge, e)
                try:
                    log_fetch(
                        conn, run_id, meta.rsge, "error",
                        error_message=str(e)[:500],
                        duration_ms=int((time.time() - start_ms) * 1000),
                    )
                    conn.commit()
                except Exception:
                    log.exception("Failed to write error log for %s", meta.rsge)

            time.sleep(POLITE_DELAY_S)

        log.info(
            "Run complete. changed=%d unchanged=%d errors=%d",
            changed_count, unchanged_count, error_count,
        )
        log_fetch(conn, run_id, None, "run_end", articles_parsed=len(registry))
        conn.commit()

        return 0 if error_count == 0 else 2
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
