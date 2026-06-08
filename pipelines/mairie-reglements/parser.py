#!/usr/bin/env python3
"""
Mairie-reglements parser — GE communes.

Crawls Geneva commune websites (depth-1 from homepage), extracts
regulatory text from HTML pages and PDFs, and writes to re-LLM:
  - bronze_ch.mairie_reglements_ge  (raw crawl cache)
  - knowledge_ch.documents + chunks (knowledge-ready form)

Entry point: python pipelines/mairie-reglements/parser.py

Auth (in order of precedence):
  1. RE_LLM_DB_URL env var (GitHub Actions)
  2. ~/supabase-registry/supabase-projects.json (local dev fallback)
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import os
import re
import sys
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import pdfplumber
import psycopg2
import psycopg2.extras
import requests
import trafilatura
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
SEED_CSV = REPO_ROOT / "seeds" / "mairies_ge.csv"

TARGET_PROJECT_REF = "znrvddgmczdqoucmykij"
REGISTRY_PATH = Path(
    os.getenv(
        "SUPABASE_REGISTRY_PATH",
        str(Path.home() / "supabase-registry" / "supabase-projects.json"),
    )
)

USER_AGENT = "LAMAP-data-pipeline/mairie-reglements (+https://lamap.ch)"
REQUEST_TIMEOUT_S = 30
POLITE_DELAY_MIN = 1.0
POLITE_DELAY_MAX = 2.0
MAX_PDF_BYTES = 20 * 1024 * 1024  # 20 MB
MAX_PDFS_PER_PAGE = 40  # cap depth-2 PDF fan-out per landing page

LINK_PATTERN = re.compile(
    r"reglement|r[eè]glement|amenagement|am[eé]nagement|PLQ|pr[eé]emption"
    r"|taxe|plan.?affectation|LCI|zone|urbanisme|construction|LDTR"
    r"|plan.?localis|PDQ|PDCom",
    re.IGNORECASE,
)

# A depth-1 HTML page is kept as a knowledge doc only if it carries real
# regulatory prose. Link-list / index pages (many links, little body text)
# are dropped — we harvest their depth-2 PDF links instead.
MIN_HTML_BODY_CHARS = 400
LINK_LIST_LINK_THRESHOLD = 40
LINK_LIST_TEXT_THRESHOLD = 1500

CHUNK_TARGET_SIZE = 1500
CHUNK_MIN_SIZE = 200

SOURCE_TAG = "mairie_reglements_ge"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("mairie-reglements")

# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------


def get_db_url() -> str:
    env_url = os.getenv("RE_LLM_DB_URL")
    if env_url:
        return env_url

    if REGISTRY_PATH.exists():
        registry = json.loads(REGISTRY_PATH.read_text())
        entry = registry.get("re-llm", {})
        pooler = entry.get("session_pooler_uri")
        if pooler:
            return pooler

    log.error("No DB connection found. Set RE_LLM_DB_URL or check supabase-registry.")
    sys.exit(1)


def connect() -> psycopg2.extensions.connection:
    url = get_db_url()
    log.info("Connecting to re-LLM...")
    conn = psycopg2.connect(url)
    conn.autocommit = False
    log.info("  Connected.")
    return conn


# ---------------------------------------------------------------------------
# Bronze table setup
# ---------------------------------------------------------------------------

CREATE_BRONZE_TABLE = """
CREATE TABLE IF NOT EXISTS bronze_ch.mairie_reglements_ge (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    url             text NOT NULL,
    url_hash        text NOT NULL UNIQUE,
    commune_name    text NOT NULL,
    commune_bfs     integer NOT NULL,
    page_title      text,
    content_type    text,
    raw_text        text,
    content_hash    text,
    file_size_bytes integer,
    crawl_status    text DEFAULT 'success',
    error_message   text,
    first_seen_at   timestamptz DEFAULT now(),
    last_seen_at    timestamptz DEFAULT now(),
    last_changed_at timestamptz DEFAULT now()
);
"""


def ensure_bronze_table(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_BRONZE_TABLE)
    conn.commit()
    log.info("  Bronze table ready.")


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})


def polite_sleep() -> None:
    import random
    time.sleep(random.uniform(POLITE_DELAY_MIN, POLITE_DELAY_MAX))


def fetch_page(url: str) -> requests.Response | None:
    try:
        resp = SESSION.get(url, timeout=REQUEST_TIMEOUT_S, allow_redirects=True)
        resp.raise_for_status()
        return resp
    except requests.RequestException as e:
        log.warning("  Failed to fetch %s: %s", url, e)
        return None


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Link discovery (depth-1 regulatory pages, depth-2 PDFs)
# ---------------------------------------------------------------------------


def _same_site(netloc: str, base_host: str) -> bool:
    return not netloc or netloc.endswith(base_host.replace("www.", ""))


def discover_links(homepage_url: str, resp: requests.Response | None = None) -> list[dict[str, str]]:
    """Depth-1: regex-matched links (HTML or PDF) on the homepage."""
    if resp is None:
        resp = fetch_page(homepage_url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    base_host = urlparse(homepage_url).netloc
    found: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        anchor_text = a_tag.get_text(strip=True)

        abs_url = urljoin(homepage_url, href)
        parsed = urlparse(abs_url)

        if not _same_site(parsed.netloc, base_host):
            continue

        if parsed.scheme in ("mailto", "tel", "javascript"):
            continue

        clean_url = parsed._replace(fragment="").geturl()

        if clean_url in seen_urls:
            continue

        # Match regex against URL path + anchor text
        url_text = parsed.path + " " + anchor_text
        if LINK_PATTERN.search(url_text):
            seen_urls.add(clean_url)
            found.append({"url": clean_url, "anchor": anchor_text})

    return found


def discover_pdf_links(page_url: str, resp: requests.Response) -> list[dict[str, str]]:
    """Depth-2: PDF links found on an already-matched landing page.

    Only PDFs — we never expand the general HTML crawl to depth-2.
    Capped at MAX_PDFS_PER_PAGE to avoid fan-out on index-heavy sites.
    """
    soup = BeautifulSoup(resp.text, "html.parser")
    base_host = urlparse(page_url).netloc
    found: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        anchor_text = a_tag.get_text(strip=True)

        abs_url = urljoin(page_url, href)
        parsed = urlparse(abs_url)

        if not _same_site(parsed.netloc, base_host):
            continue

        # PDFs only (extension check; some are served without .pdf but those
        # need a HEAD — extension covers the overwhelming majority on these sites)
        if not parsed.path.lower().endswith(".pdf"):
            continue

        clean_url = parsed._replace(fragment="").geturl()
        if clean_url in seen_urls:
            continue

        seen_urls.add(clean_url)
        found.append({"url": clean_url, "anchor": anchor_text})

        if len(found) >= MAX_PDFS_PER_PAGE:
            break

    return found


def is_substantive_html(resp: requests.Response, text: str | None) -> bool:
    """True if a depth-1 HTML page carries real regulatory prose (keep it),
    False if it's a thin nav / link-list index page (drop it)."""
    if not text:
        return False
    text_len = len(text.strip())
    if text_len < MIN_HTML_BODY_CHARS:
        return False
    soup = BeautifulSoup(resp.text, "html.parser")
    link_count = len(soup.find_all("a", href=True))
    if link_count > LINK_LIST_LINK_THRESHOLD and text_len < LINK_LIST_TEXT_THRESHOLD:
        return False
    return True


# ---------------------------------------------------------------------------
# Content extraction
# ---------------------------------------------------------------------------


def is_pdf_url(url: str, resp: requests.Response | None = None) -> bool:
    if url.lower().endswith(".pdf"):
        return True
    if resp and "application/pdf" in resp.headers.get("Content-Type", ""):
        return True
    return False


def extract_html(resp: requests.Response) -> tuple[str | None, str | None]:
    text = trafilatura.extract(resp.text, include_comments=False, include_tables=True)
    soup = BeautifulSoup(resp.text, "html.parser")
    title_tag = soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else None
    return text, title


def extract_pdf(resp: requests.Response, url: str) -> tuple[str | None, str | None]:
    if len(resp.content) > MAX_PDF_BYTES:
        log.warning("  Skipping large PDF (%d bytes): %s", len(resp.content), url)
        return None, None

    try:
        pdf = pdfplumber.open(io.BytesIO(resp.content))
        pages_text = []
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                pages_text.append(t)
        pdf.close()

        full_text = "\n\n".join(pages_text)
        # Title: use filename from URL
        filename = urlparse(url).path.split("/")[-1]
        title = filename.replace(".pdf", "").replace("_", " ").replace("-", " ").strip()
        return full_text if full_text.strip() else None, title or None
    except Exception as e:
        log.warning("  PDF extraction failed for %s: %s", url, e)
        return None, None


# ---------------------------------------------------------------------------
# Crawl one commune
# ---------------------------------------------------------------------------


def _ingest(
    conn, url, commune_name, bfs, title, content_type, text, resp, stats,
) -> None:
    """Upsert one extracted page/PDF to bronze and update counters."""
    if not text or len(text.strip()) < 50:
        log.info("  Skipping (too short or empty): %s", url)
        return

    url_hash = sha256(url)
    content_hash = sha256(text)
    file_size = len(resp.content)

    changed = upsert_bronze(
        conn, url, url_hash, commune_name, bfs,
        title, content_type, text, content_hash, file_size,
    )

    if changed:
        stats["pages_extracted"] += 1
        stats["pdfs"] += 1 if content_type == "pdf" else 0
    else:
        stats["pages_unchanged"] += 1


def crawl_commune(
    commune_name: str, bfs: int, homepage_url: str, conn: psycopg2.extensions.connection
) -> dict[str, int]:
    stats = {
        "links_found": 0, "pages_extracted": 0, "pages_unchanged": 0,
        "errors": 0, "pdfs": 0, "html_dropped": 0,
    }

    log.info("Crawling %s (BFS %d) — %s", commune_name, bfs, homepage_url)
    home_resp = fetch_page(homepage_url)
    if not home_resp:
        log.info("  Homepage unreachable.")
        return stats

    links = discover_links(homepage_url, resp=home_resp)
    stats["links_found"] = len(links)

    if not links:
        log.info("  No matching links found on homepage.")
        return stats

    log.info("  Found %d matching depth-1 links.", len(links))

    for link_info in links:
        url = link_info["url"]
        url_hash = sha256(url)

        polite_sleep()
        resp = fetch_page(url)

        if not resp:
            upsert_bronze_error(conn, url, url_hash, commune_name, bfs, "fetch_failed")
            stats["errors"] += 1
            continue

        # Depth-1 direct PDF link.
        if is_pdf_url(url, resp):
            text, title = extract_pdf(resp, url)
            _ingest(conn, url, commune_name, bfs, title, "pdf", text, resp, stats)
            continue

        # Depth-1 HTML landing page: (a) harvest depth-2 PDFs, then
        # (b) keep the HTML body only if it carries real regulatory prose.
        pdf_links = discover_pdf_links(url, resp)
        if pdf_links:
            log.info("  Found %d depth-2 PDF(s) on %s", len(pdf_links), url)
        for pdf_info in pdf_links:
            pdf_url = pdf_info["url"]
            polite_sleep()
            pdf_resp = fetch_page(pdf_url)
            if not pdf_resp:
                upsert_bronze_error(
                    conn, pdf_url, sha256(pdf_url), commune_name, bfs, "fetch_failed"
                )
                stats["errors"] += 1
                continue
            ptext, ptitle = extract_pdf(pdf_resp, pdf_url)
            _ingest(conn, pdf_url, commune_name, bfs, ptitle, "pdf", ptext, pdf_resp, stats)

        text, title = extract_html(resp)
        if is_substantive_html(resp, text):
            _ingest(conn, url, commune_name, bfs, title, "html", text, resp, stats)
        else:
            stats["html_dropped"] += 1
            log.info("  Dropped link-list/thin HTML page: %s", url)

    return stats


def upsert_bronze(
    conn, url, url_hash, commune_name, bfs,
    title, content_type, text, content_hash, file_size,
) -> bool:
    """Returns True if row was inserted or content changed."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bronze_ch.mairie_reglements_ge
                (url, url_hash, commune_name, commune_bfs, page_title,
                 content_type, raw_text, content_hash, file_size_bytes,
                 crawl_status, last_seen_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'success', now())
            ON CONFLICT (url_hash) DO UPDATE SET
                page_title = EXCLUDED.page_title,
                content_type = EXCLUDED.content_type,
                raw_text = CASE
                    WHEN bronze_ch.mairie_reglements_ge.content_hash IS DISTINCT FROM EXCLUDED.content_hash
                    THEN EXCLUDED.raw_text
                    ELSE bronze_ch.mairie_reglements_ge.raw_text
                END,
                content_hash = EXCLUDED.content_hash,
                file_size_bytes = EXCLUDED.file_size_bytes,
                crawl_status = 'success',
                error_message = NULL,
                last_seen_at = now(),
                last_changed_at = CASE
                    WHEN bronze_ch.mairie_reglements_ge.content_hash IS DISTINCT FROM EXCLUDED.content_hash
                    THEN now()
                    ELSE bronze_ch.mairie_reglements_ge.last_changed_at
                END
            RETURNING (xmax = 0) AS is_insert,
                      content_hash IS DISTINCT FROM %s AS content_changed
            """,
            (url, url_hash, commune_name, bfs, title,
             content_type, text, content_hash, file_size,
             content_hash),
        )
        row = cur.fetchone()
        conn.commit()

        if row:
            is_insert, content_changed = row
            if is_insert:
                log.info("  + NEW %s: %s", content_type, url)
                return True
            elif content_changed:
                log.info("  ~ UPDATED %s: %s", content_type, url)
                return True
            else:
                return False
        return True


def upsert_bronze_error(conn, url, url_hash, commune_name, bfs, error_msg) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bronze_ch.mairie_reglements_ge
                (url, url_hash, commune_name, commune_bfs, crawl_status,
                 error_message, last_seen_at)
            VALUES (%s, %s, %s, %s, 'error', %s, now())
            ON CONFLICT (url_hash) DO UPDATE SET
                crawl_status = 'error',
                error_message = EXCLUDED.error_message,
                last_seen_at = now()
            """,
            (url, url_hash, commune_name, bfs, error_msg),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Bronze → knowledge_ch (documents + chunks)
# ---------------------------------------------------------------------------


def chunk_text(text: str) -> list[str]:
    """Split text into ~1500-char chunks on paragraph boundaries."""
    paragraphs = re.split(r"\n{2,}", text.strip())
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    for para in paragraphs:
        para = para.strip()
        if not para:
            continue

        if current_len + len(para) > CHUNK_TARGET_SIZE and current:
            chunks.append("\n\n".join(current))
            current = []
            current_len = 0

        current.append(para)
        current_len += len(para)

    if current:
        last_chunk = "\n\n".join(current)
        if chunks and len(last_chunk) < CHUNK_MIN_SIZE:
            chunks[-1] += "\n\n" + last_chunk
        else:
            chunks.append(last_chunk)

    return chunks


def process_bronze_to_knowledge(conn: psycopg2.extensions.connection) -> dict[str, int]:
    stats = {"documents": 0, "chunks": 0, "skipped": 0, "unchanged": 0}

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT id, url, commune_name, commune_bfs, page_title,
                   content_type, raw_text, content_hash
            FROM bronze_ch.mairie_reglements_ge
            WHERE crawl_status = 'success'
              AND raw_text IS NOT NULL
              AND length(raw_text) >= 50
            """
        )
        rows = cur.fetchall()

    log.info("Processing %d bronze rows to knowledge...", len(rows))

    for row in rows:
        doc_type = "pdf_regulation" if row["content_type"] == "pdf" else "regulation"
        title = row["page_title"] or row["url"].split("/")[-1]
        doc_id = str(uuid.uuid5(uuid.NAMESPACE_URL, row["url"]))

        # Incremental: skip rows already built from identical content. This
        # preserves classification (document + chunks) on unchanged pages so
        # re-runs don't reset enriched rows back to 'pending'.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT (d.raw_metadata->>'content_hash') AS stored_hash,
                       EXISTS (SELECT 1 FROM knowledge_ch.chunks c
                               WHERE c.document_id = d.id) AS has_chunks
                FROM knowledge_ch.documents d
                WHERE d.id = %s
                """,
                (doc_id,),
            )
            existing = cur.fetchone()

        if existing and existing[1] and existing[0] == row["content_hash"]:
            stats["unchanged"] += 1
            continue

        doc_id = upsert_document(
            conn, row["url"], title, doc_type,
            row["commune_name"], row["commune_bfs"], row["content_type"],
            row["content_hash"],
        )
        if not doc_id:
            stats["skipped"] += 1
            continue

        stats["documents"] += 1

        text_chunks = chunk_text(row["raw_text"])

        # Content changed (or new) → rebuild chunks from scratch.
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM knowledge_ch.chunks WHERE document_id = %s",
                (doc_id,),
            )

        for i, chunk_content in enumerate(text_chunks):
            insert_chunk(conn, doc_id, i, chunk_content)
            stats["chunks"] += 1

        conn.commit()

    return stats


def upsert_document(
    conn, url, title, doc_type, commune_name, commune_bfs, content_type,
    content_hash,
) -> str | None:
    doc_id = str(uuid.uuid5(uuid.NAMESPACE_URL, url))
    raw_meta = json.dumps({
        "commune_bfs": commune_bfs,
        "content_type": content_type,
        "content_hash": content_hash,
    })

    # Reached only for new or content-changed docs (caller filters unchanged),
    # so resetting categorization_status to 'pending' on conflict is correct —
    # changed content must be re-classified.
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO knowledge_ch.documents
                (id, title, source, original_url, document_type, publisher,
                 language, country, canton_code, domain, accessible_to_products,
                 categorization_status, is_active, raw_metadata,
                 created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s,
                    'fr', 'CH', 'GE', 'real_estate', '{lamap,lbi}',
                    'pending', true, %s::jsonb,
                    now(), now())
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                document_type = EXCLUDED.document_type,
                raw_metadata = EXCLUDED.raw_metadata,
                categorization_status = 'pending',
                updated_at = now()
            RETURNING id
            """,
            (doc_id, title, SOURCE_TAG, url, doc_type, commune_name, raw_meta),
        )
        result = cur.fetchone()
        return result[0] if result else None


def insert_chunk(conn, doc_id: str, chunk_index: int, content: str) -> None:
    chunk_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{doc_id}::{chunk_index}"))
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO knowledge_ch.chunks
                (id, document_id, chunk_index, content,
                 domain, accessible_to_products, categorization_status,
                 created_at)
            VALUES (%s, %s, %s, %s,
                    'real_estate', '{lamap,lbi}', 'pending',
                    now())
            ON CONFLICT (id) DO UPDATE SET
                content = EXCLUDED.content,
                categorization_status = 'pending'
            """,
            (chunk_id, doc_id, chunk_index, content),
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def load_seed() -> list[dict[str, Any]]:
    if not SEED_CSV.exists():
        log.error("Seed CSV not found: %s", SEED_CSV)
        sys.exit(1)

    communes = []
    with open(SEED_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            communes.append({
                "commune_name": row["commune_name"],
                "bfs": int(row["bfs"]),
                "homepage_url": row["homepage_url"],
            })

    log.info("Loaded %d communes from seed CSV.", len(communes))
    return communes


def main() -> None:
    communes = load_seed()
    conn = connect()

    ensure_bronze_table(conn)

    # Phase 1: crawl
    total_stats = {
        "links_found": 0, "pages_extracted": 0, "pages_unchanged": 0,
        "errors": 0, "pdfs": 0, "html_dropped": 0,
    }
    communes_crawled = 0

    for commune in communes:
        try:
            stats = crawl_commune(
                commune["commune_name"],
                commune["bfs"],
                commune["homepage_url"],
                conn,
            )
            for k in total_stats:
                total_stats[k] += stats[k]
            communes_crawled += 1
        except Exception as e:
            log.error("Failed to crawl %s: %s", commune["commune_name"], e)

    log.info("")
    log.info("=== CRAWL SUMMARY ===")
    log.info("Communes crawled:   %d / %d", communes_crawled, len(communes))
    log.info("Depth-1 links found:%d", total_stats["links_found"])
    log.info("Pages extracted:    %d (of which PDFs: %d)", total_stats["pages_extracted"], total_stats["pdfs"])
    log.info("Pages unchanged:    %d", total_stats["pages_unchanged"])
    log.info("HTML link-lists dropped: %d", total_stats["html_dropped"])
    log.info("Errors:             %d", total_stats["errors"])

    # Phase 2: bronze → knowledge
    log.info("")
    log.info("=== KNOWLEDGE PROCESSING ===")
    k_stats = process_bronze_to_knowledge(conn)
    log.info("Documents created/updated: %d", k_stats["documents"])
    log.info("Chunks created:            %d", k_stats["chunks"])
    log.info("Unchanged (skipped):       %d", k_stats["unchanged"])
    log.info("Errors (no doc id):        %d", k_stats["skipped"])

    conn.close()
    log.info("Done.")


if __name__ == "__main__":
    main()
