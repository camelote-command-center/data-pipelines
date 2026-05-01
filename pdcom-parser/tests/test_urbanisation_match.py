"""Unit tests for the v0.5 urbanisation Tier 1/2/3 label matcher."""
from pdcom_parser.urbanisation_extractor import (
    match_label_to_category,
    normalize_for_match,
)


# ─── normalize_for_match ───────────────────────────────────────────────────────

def test_normalize_strips_accents_and_lowercases():
    assert normalize_for_match("Périmètre A") == "perimetre a"


def test_normalize_preserves_parenthesized_clause():
    """Parens are PRESERVED — the MZ category disambiguator (court vs long)
    lives inside them. Only the trailing 'selon fiche A0X' boilerplate is dropped."""
    out = normalize_for_match("MZ (court – moyen terme)")
    assert "court" in out
    assert "moyen" in out
    assert "(" in out and ")" in out


def test_normalize_drops_pdcn_boilerplate():
    out = normalize_for_match("développement par MZ, selon fiche A03 du PDCn")
    assert "fiche" not in out
    assert "pdcn" not in out
    assert "mz" in out


def test_normalize_collapses_dashes_and_whitespace():
    out = normalize_for_match("court  —  moyen   terme")
    assert out == "court - moyen terme"


def test_normalize_handles_empty():
    assert normalize_for_match("") == ""
    assert normalize_for_match("   ") == ""


# ─── Tier 1 (exact normalized equality) ────────────────────────────────────────

def test_tier1_plq_canonical_label():
    m = match_label_to_category("Secteur à développer (PLQ) à établir / existant")
    assert m is not None
    assert m.tier == 1
    assert m.confidence == 0.95
    assert m.key == "plq_a_etablir_existant"


def test_tier1_planification_imperative_canonical():
    m = match_label_to_category("Périmètre A : planification impérative")
    assert m is not None
    assert m.tier == 1
    assert m.key == "planification_imperative"


# ─── Tier 2 (primary + require + reject + partial_ratio) ───────────────────────

def test_tier2_plq_variation_existant_only():
    """PLQ existant — should match plq_a_etablir_existant via primary 'plq'."""
    m = match_label_to_category("PLQ existant")
    assert m is not None
    assert m.key == "plq_a_etablir_existant"


def test_tier2_mz_court_disambiguates_via_require():
    """'MZ court terme' should match court_moyen, not moyen_long, because
    'court' is required and 'long' is rejected."""
    m = match_label_to_category(
        "Périmètre de planification du développement par MZ (court terme), selon fiche A03 du PDCn"
    )
    assert m is not None
    assert m.key == "mz_court_moyen_terme"


def test_tier2_mz_long_disambiguates_via_require():
    """'MZ long terme' should match moyen_long, not court_moyen."""
    m = match_label_to_category(
        "Périmètre de planification du développement par MZ (moyen - long terme), selon fiche A03 du PDCn"
    )
    assert m is not None
    assert m.key == "mz_moyen_long_terme"


def test_tier2_densification_accrue():
    m = match_label_to_category("Densification accrue (zone 5)")
    assert m is not None
    assert m.key == "densification_accrue"


def test_tier2_a_proteger_a_menager():
    m = match_label_to_category("Secteur à protéger, à ménager")
    assert m is not None
    assert m.key == "a_proteger_a_menager"


# ─── Tier 3 (drop) ─────────────────────────────────────────────────────────────

def test_tier3_off_topic_label_drops():
    """Basemap clutter — should not match any category."""
    assert match_label_to_category("Bâtiment existant (sitg)") is None


def test_tier3_too_short_label_drops():
    """Min length 6 chars after normalization."""
    assert match_label_to_category("MZ") is None
    assert match_label_to_category("PLQ") is None  # only 3 chars after normalize → Tier 2 reject


def test_tier3_empty_drops():
    assert match_label_to_category("") is None
    assert match_label_to_category(None) is None  # type: ignore[arg-type]


def test_tier3_mz_without_required_disambiguator():
    """MZ but no 'court' or 'long' — both candidates fail require_one_of, drop."""
    # 'mz seul' — primary hits 'mz', but neither 'court' nor 'long' present
    assert match_label_to_category(
        "Périmètre de planification par MZ"
    ) is None


# ─── Reject-list discipline ────────────────────────────────────────────────────

def test_court_moyen_rejects_when_long_appears():
    """A label containing 'long' must NOT match mz_court_moyen_terme even
    if 'court' is also present (reject_if_contains)."""
    # Use the moyen_long canonical — has 'long' so court_moyen rejects.
    m = match_label_to_category(
        "MZ (moyen - long terme)"
    )
    assert m is not None
    # Must be moyen_long, NEVER court_moyen
    assert m.key == "mz_moyen_long_terme"
