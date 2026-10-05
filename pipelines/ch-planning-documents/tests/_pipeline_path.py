"""Put the pipeline directory on sys.path. Import this before any pipeline module.

The test modules here import the pipeline's own modules by bare name
(`import vd_pdcom_text`, `import mixed_text`, ...), which only works if the
pipeline directory is on sys.path. Unittest discovery puts *this* directory
there, not its parent, so the path has to be added explicitly.

It used to be added by a sys.path.insert inside test_parser.py. That made every
other test module depend on load order: unittest imports modules in filename
order, so a module only found the pipeline if its name sorted after
"test_parser". test_allaman_text, test_epalinges_diagnostic,
test_historical_former_commune and test_mixed_text sort before it, were imported
first, and died with ModuleNotFoundError — the "Verify parser contracts" step
went red, acquisition was skipped for the whole national scope, and the alert
read "Parser ch_planning_document_text en erreur" (run 37331861426).

Importing this module instead makes the path independent of load order, and
works the same under `unittest discover`, pytest, or running a test file
directly, because all three put this directory on sys.path. test_module_isolation
fails the build if a test module skips it.
"""

import sys
from pathlib import Path

_PIPELINE_DIR = str(Path(__file__).resolve().parents[1])
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)
