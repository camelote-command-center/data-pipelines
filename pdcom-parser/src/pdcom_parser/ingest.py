from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from pathlib import Path

import fitz
import yaml
from rapidfuzz import fuzz, process

from .normalize import normalize_commune_name


@dataclass
class IngestMatch:
    commune_bfs: int | None
    commune_name: str | None
    status: str  # matched | needs_review | unmatched
    score: float
    source: str  # filename | parent_folder | first_page_text
    candidates: list[tuple[str, float, int]] = field(default_factory=list)


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _extract_tokens(stem: str) -> str:
    s = re.sub(r"\b\d{4}\b", " ", stem)
    s = re.sub(r"\b20[12]\d\b", " ", s)
    s = re.sub(r"(?i)pdcom|plan[_\- ]?directeur|communal|rapport|synthese|synthèse|version|approuv[eé]{1,2}|ce|maj|broch\d*|annexes?|energ(?:i|y)e?s?|pietons|piétons|environnement|complet|z5", " ", s)
    s = re.sub(r"[_\-\.]+", " ", s).strip()
    return s


def match_pdf_to_commune(pdf_path: Path, communes: list[dict], threshold: int = 85) -> IngestMatch:
    stem_tokens = _extract_tokens(pdf_path.stem)
    parent_tokens = _extract_tokens(pdf_path.parent.name) if pdf_path.parent.name.lower() != "pdcom" else ""

    choices = {c["commune_name"]: (normalize_commune_name(c["commune_name"]), c["commune_bfs"]) for c in communes}
    norm_choices = {v[0]: (name, v[1]) for name, v in choices.items()}

    def _match(query: str) -> tuple[str, int, int] | None:
        if not query:
            return None
        q = normalize_commune_name(query)
        results = process.extract(q, list(norm_choices.keys()), scorer=fuzz.WRatio, limit=3)
        if not results:
            return None
        top = results[0]
        return (norm_choices[top[0]][0], int(top[1]), norm_choices[top[0]][1])

    candidates = []

    # Try filename first
    r = _match(stem_tokens)
    if r:
        candidates.append(("filename", *r))
    if parent_tokens:
        r = _match(parent_tokens)
        if r:
            candidates.append(("parent_folder", *r))

    # First-page text: look for "Commune de XXX"
    try:
        with fitz.open(pdf_path) as pdf:
            if pdf.page_count:
                text = pdf[0].get_text()[:2000]
                m = re.search(r"commune\s+de\s+([A-ZÉÈÊÀÂÎÔÛa-zéèêàâîôûü\-' ]{3,40})", text, re.IGNORECASE)
                if m:
                    r = _match(m.group(1).strip())
                    if r:
                        candidates.append(("first_page_text", *r))
    except Exception:
        pass

    # Pick best candidate
    if not candidates:
        return IngestMatch(
            commune_bfs=None, commune_name=None, status="unmatched",
            score=0, source="none", candidates=[],
        )
    # Sort by score desc
    candidates.sort(key=lambda c: -c[2])
    best = candidates[0]
    best_source, best_name, best_score, best_bfs = best
    # If top two are both strong & different → needs_review
    cand_list = [(c[1], c[2], c[3]) for c in candidates]
    if best_score < threshold:
        return IngestMatch(
            commune_bfs=None, commune_name=best_name, status="unmatched",
            score=best_score, source=best_source, candidates=cand_list,
        )
    # Check ambiguity
    distinct = {c[3] for c in candidates if c[2] >= threshold}
    if len(distinct) > 1:
        return IngestMatch(
            commune_bfs=best_bfs, commune_name=best_name, status="needs_review",
            score=best_score, source=best_source, candidates=cand_list,
        )
    return IngestMatch(
        commune_bfs=best_bfs, commune_name=best_name, status="matched",
        score=best_score, source=best_source, candidates=cand_list,
    )


# v0.3: atlas auto-skip removed. In v0.2.1 we skipped any filename containing
# "atlas" as canton-level, but Ilan's hints confirm the Genève atlas has 52
# extractable commune-scale map pages. Let name-matching handle it — the
# "geneve_2e_atlas_transition" filename still fuzzy-matches to Genève (bfs 12099).
# Keeping `pdcant` (Plan directeur cantonal) in the list because those are
# unambiguously canton-wide planning docs not tied to a single commune.
_ATLAS_KEYWORDS = ("plan_directeur_cantonal", "pdcant")


def _is_canton_atlas(path: Path) -> bool:
    name = path.stem.lower()
    return any(kw in name for kw in _ATLAS_KEYWORDS)


def build_ingest_manifest(pdf_dir: Path, communes: list[dict]) -> dict:
    manifest: dict = {"matched": {}, "needs_review": [], "unmatched": [], "canton_atlas": []}
    pdf_files = sorted(pdf_dir.rglob("*.pdf"))
    for path in pdf_files:
        if _is_canton_atlas(path):
            manifest["canton_atlas"].append({
                "path": str(path),
                "sha256": sha256_of(path),
                "size_bytes": path.stat().st_size,
                "reason": "canton_atlas_not_commune_pdcom",
            })
            continue
        m = match_pdf_to_commune(path, communes)
        rec = {
            "path": str(path),
            "sha256": sha256_of(path),
            "size_bytes": path.stat().st_size,
            "score": m.score,
            "source": m.source,
            "candidates": m.candidates,
        }
        if m.status == "matched":
            bfs = m.commune_bfs
            slot = manifest["matched"].setdefault(bfs, {"commune_name": m.commune_name, "pdfs": []})
            slot["pdfs"].append(rec)
        elif m.status == "needs_review":
            rec["commune_bfs"] = m.commune_bfs
            rec["commune_name"] = m.commune_name
            rec["reason"] = "multiple candidates above threshold"
            manifest["needs_review"].append(rec)
        else:
            rec["reason"] = "no match above threshold"
            manifest["unmatched"].append(rec)
    return manifest


def write_manifest_yaml(manifest: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Flatten matched keys to strings for YAML
    out = {
        "matched": {},
        "needs_review": manifest["needs_review"],
        "unmatched": manifest["unmatched"],
        "canton_atlas": manifest.get("canton_atlas", []),
    }
    for bfs, data in manifest["matched"].items():
        out["matched"][int(bfs)] = {"name": data["commune_name"], "pdfs": data["pdfs"]}
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(out, f, allow_unicode=True, sort_keys=True)
