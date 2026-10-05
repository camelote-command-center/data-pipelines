"""Put the pipeline directory on sys.path. Import this before any pipeline module.

Unittest discovery puts only the tests directory on sys.path, so a test module
that does `import vevey_access` needs the pipeline directory added. Most modules
here do it themselves with sys.path.insert; 23 did not, and worked only because
unittest imports modules in filename order and some earlier module had already
inserted the path. That is a trap, not a mechanism: rename a file, or add one
sorting before test_active_qualifications.py, and those 23 fail to import — the
"Verify parser contracts" step goes red and acquisition is skipped for the whole
pipeline.

That is exactly how ch-planning-documents broke on 2026-10-05 (run 37331861426,
alert "Parser ch_planning_document_text en erreur"): four test modules added with
names sorting before the one holding the insert. test_path_bootstrap keeps it
from happening here.
"""

import sys
from pathlib import Path

_PIPELINE_DIR = str(Path(__file__).resolve().parents[1])
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)
