"""
INSEE Sirene API v3.11 client.

OAuth2 client_credentials flow — token endpoint at auth.insee.fr.
We fetch a fresh token on every parser run (token TTL is 7 days, simpler than
caching). All requests carry `Authorization: Bearer <token>`.

Pagination: SIRENE caps `nombre` at 1000 results per call and `debut` (offset)
at 10000. For larger result sets, callers must page in date windows or partition
by another dimension. The incremental-sync use case here filters by
`dateDernierTraitementUniteLegale` and rarely sees >10k rows per day, so simple
offset pagination is sufficient.

Rate limit: 30 req/min (default tier). The client sleeps 2.1s between calls
as a defensive guard. If you hit a 429, the client backs off exponentially.

Environment variables:
    INSEE_CLIENT_ID
    INSEE_CLIENT_SECRET
"""

from __future__ import annotations

import os
import time
from typing import Iterable

import requests

TOKEN_URL = "https://auth.insee.fr/auth/realms/apim-gravitee/protocol/openid-connect/token"
API_BASE = "https://api.insee.fr/api-sirene/3.11"

DEFAULT_PAGE_SIZE = 1000
MAX_OFFSET = 10000
MIN_INTERVAL_S = 2.1   # ~28 req/min, under the 30/min cap
MAX_RETRIES = 5


class CredentialsMissing(RuntimeError):
    """Raised when INSEE_CLIENT_ID or INSEE_CLIENT_SECRET is unset."""


class SireneClient:
    def __init__(
        self,
        client_id: str | None = None,
        client_secret: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.client_id = client_id or os.environ.get("INSEE_CLIENT_ID", "")
        self.client_secret = client_secret or os.environ.get("INSEE_CLIENT_SECRET", "")
        if not self.client_id or not self.client_secret:
            raise CredentialsMissing(
                "INSEE_CLIENT_ID and INSEE_CLIENT_SECRET environment variables are required"
            )
        self.timeout = timeout
        self._token: str | None = None
        self._last_call_at: float = 0.0

    # ────────────────────────── auth ──────────────────────────

    def _fetch_token(self) -> str:
        r = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=self.timeout,
        )
        r.raise_for_status()
        return r.json()["access_token"]

    def _ensure_token(self) -> None:
        if self._token is None:
            self._token = self._fetch_token()

    # ────────────────────────── HTTP ──────────────────────────

    def _throttle(self) -> None:
        delta = time.monotonic() - self._last_call_at
        if delta < MIN_INTERVAL_S:
            time.sleep(MIN_INTERVAL_S - delta)
        self._last_call_at = time.monotonic()

    def get(self, path: str, params: dict | None = None) -> dict:
        self._ensure_token()
        url = f"{API_BASE}{path}"
        last_err: Exception | None = None
        for attempt in range(MAX_RETRIES):
            self._throttle()
            r = requests.get(
                url,
                params=params or {},
                headers={"Authorization": f"Bearer {self._token}"},
                timeout=self.timeout,
            )
            if r.status_code == 200:
                return r.json()
            if r.status_code == 401:
                # token expired; re-auth and retry once
                self._token = self._fetch_token()
                continue
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
