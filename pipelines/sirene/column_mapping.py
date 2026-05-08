"""
Map INSEE SIRENE source fields → bronze_fr columns.

Two source flavours:
  1. data.gouv.fr Stock parquet/CSV (column names like 'siren', 'denominationUniteLegale', etc.)
  2. api.insee.fr v3.11 JSON (nested under 'uniteLegale' / 'etablissement', with 'periodes…' history arrays)

Both flatten to the same row dicts so the upsert path is identical.

Reference docs:
  - StockUniteLegale variables: https://www.sirene.fr/static-resources/htm/v_sommaire_311.htm
  - StockEtablissement variables: https://www.sirene.fr/static-resources/htm/v_sommaire_311.htm
  - API v3.11: https://www.sirene.fr/static-resources/doc/Documentation_API_Sirene.pdf
"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

from .naf_filter import normalise as naf_normalise


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def _empty_to_none(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, str) and not v.strip():
        return None
    if isinstance(v, float):
        # parquet sometimes serialises empty cells as NaN
        if v != v:  # NaN check
            return None
    return v


def _as_text(v: Any) -> str | None:
    v = _empty_to_none(v)
    if v is None:
        return None
    return str(v).strip() or None


def _as_int(v: Any) -> int | None:
    v = _empty_to_none(v)
    if v is None:
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _as_numeric(v: Any) -> float | None:
    v = _empty_to_none(v)
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _as_date(v: Any) -> str | None:
    """Return ISO YYYY-MM-DD or None. Accepts datetime/date/string."""
    v = _empty_to_none(v)
    if v is None:
        return None
    if isinstance(v, (datetime, date)):
        d = v.date() if isinstance(v, datetime) else v
        return d.isoformat()
    s = str(v).strip()
    if not s:
        return None
    # Common SIRENE format: YYYY-MM-DD already
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return None


def _as_bool(v: Any) -> bool | None:
    v = _empty_to_none(v)
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("true", "1", "oui", "o"):
        return True
    if s in ("false", "0", "non", "n"):
        return False
    return None


# ──────────────────────────────────────────────────────────────
# Stock CSV/parquet mapping — UniteLegale
# ──────────────────────────────────────────────────────────────

def map_stock_unite_legale(row: dict) -> dict:
    """Map one row from StockUniteLegale_utf8 (CSV or parquet) into a
    bronze_fr.companies record. Returns a dict ready for batch_upsert."""
    siren = _as_text(row.get("siren"))
    if not siren:
        return {}

    denom = (
        _as_text(row.get("denominationUniteLegale"))
        or _as_text(row.get("denominationUsuelle1UniteLegale"))
        or _as_text(row.get("nomUniteLegale"))
    )
    activite = naf_normalise(_as_text(row.get("activitePrincipaleUniteLegale")))

    return {
        "siren": siren,
        "siret_siege": _as_text(row.get("siretSiegeUniteLegale")),
        "denomination": denom,
        "denomination_usuelle": _as_text(row.get("denominationUsuelle1UniteLegale")),
        "sigle": _as_text(row.get("sigleUniteLegale")),
        "forme_juridique_code": _as_text(row.get("categorieJuridiqueUniteLegale")),
        "forme_juridique_libelle": None,  # not in stock CSV
        "activite_principale": activite,
        "activite_principale_libelle": None,  # not in stock CSV; resolved via separate lookup if needed
        "activite_principale_naf25": None,    # NAF2025 not in current stock files
        "capital_social": None,                # not in stock CSV
        "date_immatriculation": _as_date(row.get("dateDebut")),
        "date_creation": _as_date(row.get("dateCreationUniteLegale")),
        "date_radiation": (
            _as_date(row.get("dateFin"))
            if _as_text(row.get("etatAdministratifUniteLegale")) == "C"
            else None
        ),
        "etat_administratif": _as_text(row.get("etatAdministratifUniteLegale")),
        "adresse_ligne_1": None,
        "code_postal": None,
        "commune": None,
        "code_commune": None,
        "departement": None,
        "region": None,
        "latitude": None,
        "longitude": None,
        "nombre_etablissements": _as_int(row.get("nombrePeriodesUniteLegale")),
        "representants": None,
        "data_source": "insee_sirene",
        "raw_data": json.dumps(
            {k: v for k, v in row.items() if v is not None and v != ""},
            ensure_ascii=False,
            default=str,
        ),
    }


# ──────────────────────────────────────────────────────────────
# Stock CSV/parquet mapping — Etablissement
# ──────────────────────────────────────────────────────────────

def map_stock_etablissement(row: dict) -> dict:
    """Map one row from StockEtablissement_utf8 into bronze_fr.etablissements."""
    siret = _as_text(row.get("siret"))
    siren = _as_text(row.get("siren"))
    if not siret or not siren:
        return {}

    activite = naf_normalise(_as_text(row.get("activitePrincipaleEtablissement")))

    coord_x = _as_numeric(row.get("coordonneeLambertAbscisseEtablissement"))
    coord_y = _as_numeric(row.get("coordonneeLambertOrdonneeEtablissement"))
    # Note: stock CSV uses Lambert-93, NOT WGS84. We store NULL for lat/lon
    # from the stock pull; the API returns WGS84 directly so incremental sync
    # populates these fields. Lambert→WGS84 conversion is a silver-layer task.

    return {
        "siret": siret,
        "siren": siren,
        "nic": _as_text(row.get("nic")),
        "denomination": _as_text(row.get("denominationUsuelleEtablissement")),
        "enseigne": _as_text(row.get("enseigne1Etablissement")),
        "activite_principale": activite,
        "activite_principale_libelle": None,
        "activite_principale_naf25": None,
        "numero_voie": _as_text(row.get("numeroVoieEtablissement")),
        "type_voie": _as_text(row.get("typeVoieEtablissement")),
        "libelle_voie": _as_text(row.get("libelleVoieEtablissement")),
        "complement_adresse": _as_text(row.get("complementAdresseEtablissement")),
        "code_postal": _as_text(row.get("codePostalEtablissement")),
        "commune": _as_text(row.get("libelleCommuneEtablissement")),
        "code_commune": _as_text(row.get("codeCommuneEtablissement")),
        "departement": (
            (_as_text(row.get("codeCommuneEtablissement")) or "")[:2]
            or None
        ),
        "region": None,  # not in stock CSV; can be derived from departement in silver
        "latitude": None,    # see Lambert-93 note above
        "longitude": None,
        "etat_administratif": _as_text(row.get("etatAdministratifEtablissement")),
        "date_creation": _as_date(row.get("dateCreationEtablissement")),
        "date_debut_activite": _as_date(row.get("dateDebut")),
        "date_fermeture": (
            _as_date(row.get("dateFin"))
            if _as_text(row.get("etatAdministratifEtablissement")) == "F"
            else None
        ),
        "est_siege": _as_bool(row.get("etablissementSiege")) or False,
        "tranche_effectifs": _as_text(row.get("trancheEffectifsEtablissement")),
        "tranche_effectifs_libelle": None,
        "caractere_employeur": _as_text(row.get("caractereEmployeurEtablissement")),
        "data_source": "insee_sirene",
        "raw_data": json.dumps(
            {k: v for k, v in row.items() if v is not None and v != ""},
            ensure_ascii=False,
            default=str,
        ),
    }


# ──────────────────────────────────────────────────────────────
# API v3.11 mapping — UniteLegale
# ──────────────────────────────────────────────────────────────

def _api_current_period(periodes: list[dict] | None) -> dict:
    """The 'periodes' array on API responses lists each historical state.
    The most-recent period (no dateFin OR latest dateDebut) is the current one.
    """
    if not periodes:
        return {}
    open_periods = [p for p in periodes if not p.get("dateFin")]
    if open_periods:
        return open_periods[0]
    return max(periodes, key=lambda p: p.get("dateDebut") or "")


def map_api_unite_legale(payload: dict) -> dict:
    """Map an API /siren response (`{"uniteLegale": {...}}`) into a row."""
    ul = payload.get("uniteLegale") or payload
    if not ul:
        return {}
    siren = _as_text(ul.get("siren"))
    if not siren:
        return {}

    cur = _api_current_period(ul.get("periodesUniteLegale"))
    activite = naf_normalise(_as_text(cur.get("activitePrincipaleUniteLegale")))

    return {
        "siren": siren,
        "siret_siege": _as_text(ul.get("siretSiegeUniteLegale")),
        "denomination": _as_text(cur.get("denominationUniteLegale")),
        "denomination_usuelle": _as_text(cur.get("denominationUsuelle1UniteLegale")),
        "sigle": _as_text(ul.get("sigleUniteLegale")),
        "forme_juridique_code": _as_text(cur.get("categorieJuridiqueUniteLegale")),
        "forme_juridique_libelle": None,
        "activite_principale": activite,
        "activite_principale_libelle": None,
        "activite_principale_naf25": None,
        "capital_social": None,
        "date_immatriculation": _as_date(cur.get("dateDebut")),
        "date_creation": _as_date(ul.get("dateCreationUniteLegale")),
        "date_radiation": (
            _as_date(cur.get("dateFin"))
            if _as_text(cur.get("etatAdministratifUniteLegale")) == "C"
            else None
        ),
        "etat_administratif": _as_text(cur.get("etatAdministratifUniteLegale")),
        "data_source": "insee_sirene",
        "raw_data": json.dumps(payload, ensure_ascii=False, default=str),
    }


# ──────────────────────────────────────────────────────────────
# API v3.11 mapping — Etablissement
# ──────────────────────────────────────────────────────────────

def map_api_etablissement(payload: dict) -> dict:
    """Map an API /siret response (`{"etablissement": {...}}`) into a row."""
    et = payload.get("etablissement") or payload
    if not et:
        return {}
    siret = _as_text(et.get("siret"))
    siren = _as_text(et.get("siren"))
    if not siret or not siren:
        return {}

    cur = _api_current_period(et.get("periodesEtablissement"))
    adresse = et.get("adresseEtablissement") or {}

    activite = naf_normalise(_as_text(cur.get("activitePrincipaleEtablissement")))

    return {
        "siret": siret,
        "siren": siren,
        "nic": _as_text(et.get("nic")),
        "denomination": _as_text(cur.get("denominationUsuelleEtablissement")),
        "enseigne": _as_text(cur.get("enseigne1Etablissement")),
        "activite_principale": activite,
        "activite_principale_libelle": None,
        "activite_principale_naf25": None,
        "numero_voie": _as_text(adresse.get("numeroVoieEtablissement")),
        "type_voie": _as_text(adresse.get("typeVoieEtablissement")),
        "libelle_voie": _as_text(adresse.get("libelleVoieEtablissement")),
        "complement_adresse": _as_text(adresse.get("complementAdresseEtablissement")),
        "code_postal": _as_text(adresse.get("codePostalEtablissement")),
        "commune": _as_text(adresse.get("libelleCommuneEtablissement")),
        "code_commune": _as_text(adresse.get("codeCommuneEtablissement")),
        "departement": (
            (_as_text(adresse.get("codeCommuneEtablissement")) or "")[:2] or None
        ),
        "region": None,
        # API returns WGS84 directly when geocoded
        "latitude": _as_numeric(adresse.get("latitude")),
        "longitude": _as_numeric(adresse.get("longitude")),
        "etat_administratif": _as_text(cur.get("etatAdministratifEtablissement")),
        "date_creation": _as_date(et.get("dateCreationEtablissement")),
        "date_debut_activite": _as_date(cur.get("dateDebut")),
        "date_fermeture": (
            _as_date(cur.get("dateFin"))
            if _as_text(cur.get("etatAdministratifEtablissement")) == "F"
            else None
        ),
        "est_siege": _as_bool(et.get("etablissementSiege")) or False,
        "tranche_effectifs": _as_text(et.get("trancheEffectifsEtablissement")),
        "tranche_effectifs_libelle": None,
        "caractere_employeur": _as_text(cur.get("caractereEmployeurEtablissement")),
        "data_source": "insee_sirene",
        "raw_data": json.dumps(payload, ensure_ascii=False, default=str),
    }
