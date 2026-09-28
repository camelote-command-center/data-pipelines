"""Every third-party module this pipeline imports must be declared in requirements.txt.

Why this test exists: on 2026-09-28 a bridge module imported PyMuPDF, which the workflow never
installed. The contract step ("Verify parser contracts") then failed with ModuleNotFoundError and
the acquisition step was skipped, so the scheduled run acquired nothing and alerted the founder as
"Parser ch_planning_document_text en erreur". A missing declaration must fail here, in a fast local
test, instead of silently stopping the national acquisition.
"""
import ast
import pathlib
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]

# import name -> distribution name as pinned in requirements.txt
DISTRIBUTION = {
    "requests": "requests",
    "bs4": "beautifulsoup4",
    "psycopg2": "psycopg2-binary",
    "pypdf": "pypdf",
    "defusedxml": "defusedxml",
    "fitz": "PyMuPDF",
    "pymupdf": "PyMuPDF",
    "shapely": "shapely",
}


def top_level_imports(path):
    """Every module imported by a file, including imports inside functions."""
    tree = ast.parse(path.read_text(), filename=str(path))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            names.add(node.module.split(".")[0])
    return names


class RequirementsAreComplete(unittest.TestCase):
    def test_every_third_party_import_is_declared(self):
        declared = {
            line.split("==")[0].split(">=")[0].strip().lower()
            for line in (ROOT / "requirements.txt").read_text().splitlines()
            if line.strip() and not line.startswith("#")
        }
        local = {p.stem for p in ROOT.glob("*.py")}
        missing = {}
        for path in sorted(ROOT.glob("*.py")):
            for name in sorted(top_level_imports(path)):
                if name in sys.stdlib_module_names or name in local:
                    continue
                dist = DISTRIBUTION.get(name)
                self.assertIsNotNone(
                    dist,
                    f"{path.name} imports '{name}', which this test does not know: add it to "
                    f"DISTRIBUTION with the distribution name to pin in requirements.txt",
                )
                if dist.lower() not in declared:
                    missing.setdefault(dist, []).append(path.name)
        self.assertEqual(
            missing, {},
            "requirements.txt is missing packages the pipeline imports — the workflow would fail "
            "its contract step and skip acquisition entirely: "
            + ", ".join(f"{d} (used by {', '.join(f)})" for d, f in missing.items()),
        )


if __name__ == "__main__":
    unittest.main()
