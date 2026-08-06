"""
NAF (Nomenclature d'Activités Française) Rev.2 codes for the real-estate
universe Lamap cares about. Used both for bulk-load filtering and for
labelling rows post-fetch (incremental sync does NOT pre-filter — a company
can change INTO real estate over time).

INSEE stores NAF codes as 5-character strings without dots: '6810Z' for
"Activités des marchands de biens immobiliers". The display form is "68.10Z".
This module normalises both.
"""

from __future__ import annotations

# Real estate sector (NAF division 68)
REAL_ESTATE_SECTOR = {
    "6810Z",  # Activités des marchands de biens immobiliers
    "6820A",  # Location de logements
    "6820B",  # Location de terrains et d'autres biens immobiliers
    "6831Z",  # Agences immobilières
    "6832A",  # Administration d'immeubles et autres biens immobiliers
    "6832B",  # Supports juridiques de gestion de patrimoine immobilier
}

# Real-estate development (NAF division 41 — construction de bâtiments)
REAL_ESTATE_DEVELOPMENT = {
    "4110A",  # Promotion immobilière de logements
    "4110B",  # Promotion immobilière de bureaux
    "4110C",  # Promotion immobilière d'autres bâtiments
    "4110D",  # Supports juridiques de programmes
    "4120A",  # Construction de maisons individuelles
    "4120B",  # Construction d'autres bâtiments
}

# Holdings & financial vehicles often used for real estate
HOLDINGS = {
    "6420Z",  # Activités des sociétés holding
}

# Full whitelist (set of normalised codes, no dot)
NAF_WHITELIST: set[str] = REAL_ESTATE_SECTOR | REAL_ESTATE_DEVELOPMENT | HOLDINGS


def normalise(code: str | None) -> str | None:
    """Return a normalised NAF code (no dot, uppercase) or None.

    Accepts: '68.10Z', '6810Z', '68.10z', '  6810Z  '. Returns '6810Z'.
    """
    if not code:
        return None
    s = str(code).strip().upper().replace(".", "").replace(" ", "")
    return s or None


def is_real_estate(code: str | None) -> bool:
    """True if the (raw or normalised) NAF code is in the RE whitelist."""
    norm = normalise(code)
    return norm is not None and norm in NAF_WHITELIST


def labels_for(code: str | None) -> dict[str, str | None]:
    """Convenience: return short categorisation hints for downstream use."""
    norm = normalise(code)
    if not norm:
        return {"sector": None, "naf_normalised": None}
    if norm in REAL_ESTATE_SECTOR:
        sector = "real_estate_sector"
    elif norm in REAL_ESTATE_DEVELOPMENT:
        sector = "real_estate_development"
    elif norm in HOLDINGS:
        sector = "holdings"
    else:
        sector = "other"
    return {"sector": sector, "naf_normalised": norm}


def solr_clause(field: str = "activitePrincipaleUniteLegale") -> str:
    """Return a Sirene query clause matching any whitelisted NAF code.

    Syntax notes (verified live 2026-08-06 — all three details are required):
      * `activitePrincipale*` is a HISTORISED field, so it must be wrapped in
        `periode(...)`. Unwrapped returns HTTP 400 "Erreur de syntaxe".
      * The code must be DOTTED ("68.31Z"). The undotted form stored in our
        tables ("6831Z") returns HTTP 404 / zero results.
      * The value must be QUOTED.

    Filtering server-side matters: a one-day window is ~16k changed unites
    legales against a 10k offset ceiling, but ~2.6k once this clause is applied.
    """
    dotted = sorted(f"{c[:2]}.{c[2:]}" for c in NAF_WHITELIST)
    return " OR ".join(f'periode({field}:"{d}")' for d in dotted)
