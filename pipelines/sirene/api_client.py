"""
INSEE Sirene API v3.11 client.

API-key auth (changed 2026-08-06). Sirene 3.11 does NOT use OAuth2: it takes a
single opaque API key in the `X-INSEE-Api-Key-Integration` header, issued by a
portal application created in "simple" mode and subscribed to API Sirene.

The previous OAuth2 client_credentials flow is gone. It could not work: its token
host `auth.insee.fr` no longer resolves at all, and a client_id/secret pair comes
from the "backend to backend" application mode, which INSEE documents as never
working for this API. Do not reintroduce a token exchange here.

The key is read at runtime from the Supabase vault (`insee_sirene_api_key` on
re-LLM) over HTTPS, via the service-role-only RPC public.insee_sirene_key() —
deliberately not from an env var, so the value lives in exactly one place. It is
never logged.

Pagination: SIRENE caps `nombre` at 1000 results per call and `debut` (offset)
at 10000. For larger result sets, callers must page in date windows or partition
by another dimension. The incremental-sync use case here filters by
`dateDernierTraitementUniteLegale` and rarely sees >10k rows per day, so simple
offset pagination is sufficient.

Rate limit: 30 req/min (default tier). The client sleeps 2.1s between calls
as a defensive guard. If you hit a 429, the client backs off exponentially.

Environment variables:
    RE_LLM_SUPABASE_URL              re-LLM project URL, used to read the vault key
    RE_LLM_SUPABASE_SERVICE_ROLE_KEY service role, used to call insee_sirene_key()
    INSEE_SIRENE_API_KEY             optional override, for local testing only
"""

from __future__ import annotations

import os
import time
from typing import Iterable

import requests

API_BASE = "https://api.insee.fr/api-sirene/3.11"
API_KEY_HEADER = "X-INSEE-Api-Key-Integration"
VAULT_SECRET_NAME = "insee_sirene_api_key"

DEFAULT_PAGE_SIZE = 1000
MAX_OFFSET = 10000
MIN_INTERVAL_S = 2.1   # ~28 req/min, under the 30/min cap
MAX_RETRIES = 5


class CredentialsMissing(RuntimeError):
    """Raised when the INSEE Sirene API key cannot be resolved."""


def _api_key_from_vault() -> str:
    """Read the API key from the re-LLM vault over HTTPS. Never logs the value.

    Uses the same PostgREST + service-role path the pipeline already uses for every
    other call, via public.insee_sirene_key() (SECURITY DEFINER, EXECUTE granted to
    service_role only — the existing public.nocodb_secret() convention).

    This used to open a psycopg2 connection to RE_LLM_DATABASE_URL. That worked from a
    workstation and failed on every GitHub-hosted run with
    `OperationalError: server didn't return client encoding` raised by psycopg2.connect
    itself — i.e. the startup handshake never completed from the runner's network, not
    a query-level rejection. Requiring a raw Postgres path just to read one secret gave
    the job a second, weaker dependency than the one it already needs (HTTPS), so the
    dependency is removed rather than repaired.
    """
    url = os.environ.get("RE_LLM_SUPABASE_URL", "").rstrip("/")
    key = os.environ.get("RE_LLM_SUPABASE_SERVICE_ROLE_KEY", "")
    if not url or not key:
        raise CredentialsMissing(
            "RE_LLM_SUPABASE_URL and RE_LLM_SUPABASE_SERVICE_ROLE_KEY are required to read "
            f"{VAULT_SECRET_NAME!r} from the vault "
            "(or set INSEE_SIRENE_API_KEY for local testing)"
        )
    r = requests.post(
        f"{url}/rest/v1/rpc/insee_sirene_key",
        headers={"apikey": key, "Authorization": f"Bearer {key}",
                 "Content-Type": "application/json"},
        json={}, timeout=30,
    )
    if r.status_code != 200:
        # Deliberately does not echo the body: it would contain the secret on success
        # and is not needed to diagnose a failure.
        raise CredentialsMissing(
            f"vault RPC insee_sirene_key returned HTTP {r.status_code} "
            f"(expected 200); check the service-role grant on re-LLM"
        )
    secret = (r.json() or "")
    if not isinstance(secret, str) or not secret.strip():
        raise CredentialsMissing(f"vault secret {VAULT_SECRET_NAME!r} is empty on re-LLM")
    return secret.strip()


class SireneClient:
    def __init__(
        self,
        api_key: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        # Precedence: explicit arg > env override (local testing) > vault.
        self.api_key = api_key or os.environ.get("INSEE_SIRENE_API_KEY", "").strip()
        if not self.api_key:
            self.api_key = _api_key_from_vault()
        self.timeout = timeout
        self._last_call_at: float = 0.0

    # ────────────────────────── HTTP ──────────────────────────

    def _throttle(self) -> None:
        delta = time.monotonic() - self._last_call_at
        if delta < MIN_INTERVAL_S:
            time.sleep(MIN_INTERVAL_S - delta)
        self._last_call_at = time.monotonic()

    def get(self, path: str, params: dict | None = None) -> dict:
        url = f"{API_BASE}{path}"
        last_err: Exception | None = None
        for attempt in range(MAX_RETRIES):
            self._throttle()
            r = requests.get(
                url,
                params=params or {},
                headers={API_KEY_HEADER: self.api_key},
                timeout=self.timeout,
            )
            if r.status_code == 200:
                return r.json()
            if r.status_code == 401:
                # Static API key: a 401 means the key is wrong, revoked, or the
                # subscription lapsed. There is nothing to refresh, so fail loudly
                # rather than burning retries on a credential that cannot recover.
                raise CredentialsMissing(
                    "INSEE rejected the API key (401). Check that the vault secret "
                    f"{VAULT_SECRET_NAME!r} matches a live subscription to API Sirene 3.11."
                )
            if r.status_code == 404:
                # Empty result set on /siren?q=... yields 404 with a body
                # like {"header": {"statut": 404, ...}}. Treat as empty page.
                try:
                    body = r.json()
                    if (body.get("header") or {}).get("statut") == 404:
                        return body
                except ValueError:
                    pass
                raise requests.HTTPError(f"404 on {url}: {r.text[:200]}")
            if r.status_code == 429:
                wait = 2 ** attempt * 5
                print(f"  [api] 429 rate-limited; sleeping {wait}s (attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            if 500 <= r.status_code < 600:
                wait = 2 ** attempt * 2
                print(f"  [api] {r.status_code} server error; sleeping {wait}s")
                time.sleep(wait)
                last_err = requests.HTTPError(f"{r.status_code}: {r.text[:200]}")
                continue
            # Other client errors: don't retry
            raise requests.HTTPError(f"{r.status_code}: {r.text[:300]}")
        raise last_err or RuntimeError(f"max retries exhausted on {url}")

    # ────────────────────── high-level calls ─────────────────

    def count_unites_legales(self, q: str) -> int:
        """Return the total match count for `q` without pulling any pages.

        Used to detect, before importing anything, that a window would exceed the
        offset ceiling — a truncated import is worse than a failed one.
        """
        page = self.get("/siren", params={"q": q, "nombre": 1, "debut": 0})
        header = page.get("header") or {}
        if header.get("statut") == 404:
            return 0
        return int(header.get("total") or 0)

    def iter_unites_legales(
        self,
        q: str,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> Iterable[dict]:
        """Yield UniteLegale items matching `q` (Solr-style query string).

        Pages via `debut` until we exhaust results or hit MAX_OFFSET (10k).
        Caller is responsible for narrowing `q` if more than 10k matches.
        """
        debut = 0
        while debut < MAX_OFFSET:
            page = self.get(
                "/siren",
                params={"q": q, "nombre": page_size, "debut": debut},
            )
            header = page.get("header") or {}
            if header.get("statut") == 404:
                return
            items = page.get("unitesLegales") or []
            if not items:
                return
            for it in items:
                yield it
            total = header.get("total", 0)
            debut += page_size
            if debut >= total:
                return
        raise RuntimeError(
            f"Result window > {MAX_OFFSET}; narrow the query to avoid truncation. "
            f"q={q!r}"
        )

    def iter_etablissements_by_sirens(
        self,
        sirens: list[str],
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> Iterable[dict]:
        """Yield Etablissement items for a BATCH of SIRENs in one query.

        `q=siren:(A OR B OR C)` returns the union, so one call replaces N. The
        establishment phase used to issue one request per SIREN, which at the 2.1s
        rate cap cost ~92 min for a normal ~2.6k-company day against a 30 min budget.

        Batch size is bounded by the 10k offset ceiling, not by URL length: measured
        over 4.88M companies the distribution is avg 1.4 establishments, p99 = 5,
        p99.9 = 8, max 1478, and only 5 companies anywhere exceed 500. A 50-SIREN
        batch is therefore ~70 rows typically and a few thousand in the worst
        realistic case. The count is still checked below so a pathological batch is
        split rather than silently truncated.
        """
        if not sirens:
            return
        q = "siren:(" + " OR ".join(sirens) + ")"
        total = self.count_etablissements(q)
        if total >= MAX_OFFSET:
            # Too big to page safely: split and recurse rather than truncate.
            if len(sirens) == 1:
                raise RuntimeError(
                    f"SIREN {sirens[0]} alone has {total} establishments, at or above "
                    f"the {MAX_OFFSET} offset ceiling; it cannot be paged."
                )
            mid = len(sirens) // 2
            yield from self.iter_etablissements_by_sirens(sirens[:mid], page_size)
            yield from self.iter_etablissements_by_sirens(sirens[mid:], page_size)
            return
        debut = 0
        while debut < MAX_OFFSET:
            page = self.get(
                "/siret",
                params={"q": q, "nombre": page_size, "debut": debut},
            )
            header = page.get("header") or {}
            if header.get("statut") == 404:
                return
            items = page.get("etablissements") or []
            if not items:
                return
            for it in items:
                yield it
            tot = header.get("total", 0)
            debut += page_size
            if debut >= tot:
                return

    def count_etablissements(self, q: str) -> int:
        """Total match count for an /siret query, without pulling pages."""
        page = self.get("/siret", params={"q": q, "nombre": 1, "debut": 0})
        header = page.get("header") or {}
        if header.get("statut") == 404:
            return 0
        return int(header.get("total") or 0)

    def iter_etablissements_by_siren(
        self,
        siren: str,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> Iterable[dict]:
        """Yield all Etablissement items for a given SIREN."""
        debut = 0
        while debut < MAX_OFFSET:
            page = self.get(
                "/siret",
                params={"q": f"siren:{siren}", "nombre": page_size, "debut": debut},
            )
            header = page.get("header") or {}
            if header.get("statut") == 404:
                return
            items = page.get("etablissements") or []
            if not items:
                return
            for it in items:
                yield it
            total = header.get("total", 0)
            debut += page_size
            if debut >= total:
                return
