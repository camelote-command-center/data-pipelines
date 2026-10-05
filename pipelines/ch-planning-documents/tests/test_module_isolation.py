"""Every test module here must import on its own, whatever order it is loaded in.

Why this test exists: unittest discovery imports test modules in filename order
and puts only this directory on sys.path. The pipeline directory used to be added
by a sys.path.insert inside test_parser.py, so a test module could import the
pipeline's modules only if its filename sorted after "test_parser". Four modules
added later sorted before it (test_allaman_text, test_epalinges_diagnostic,
test_historical_former_commune, test_mixed_text), were imported first, and failed
with ModuleNotFoundError: the "Verify parser contracts" step went red, the
acquisition step was skipped, and the scheduled run alerted the founder as
"Parser ch_planning_document_text en erreur" (run 37331861426). The path now comes
from _pipeline_path, which every such module imports.

This test discovers each module in a subprocess of its own, exactly the way the
workflow discovers the suite, and fails if any module cannot be imported alone —
so forgetting the bootstrap in a new test, or relying on any other module's import
side effect, fails here instead of stopping the national acquisition.
"""
import os
import pathlib
import subprocess
import sys
import unittest

TESTS_DIR = pathlib.Path(__file__).resolve().parent

# Discover one module and report any that unittest could not import. unittest
# represents an unimportable module as a synthetic _FailedTest case, which is
# exactly what the workflow log showed.
CHILD = """
import sys, unittest
suite = unittest.defaultTestLoader.discover(sys.argv[1], pattern=sys.argv[2])
def flatten(s):
    for t in s:
        if isinstance(t, unittest.TestSuite):
            yield from flatten(t)
        else:
            yield t
ids = [t.id() for t in flatten(suite)]
broken = [i for i in ids if '_FailedTest' in i]
if broken:
    sys.stderr.write('unimportable: ' + ', '.join(broken) + '\\n')
    raise SystemExit(1)
if not ids:
    sys.stderr.write('no tests discovered\\n')
    raise SystemExit(2)
"""


class ModuleIsolationTests(unittest.TestCase):
    def test_every_test_module_imports_alone(self):
        modules = sorted(p.name for p in TESTS_DIR.glob('test_*.py'))
        self.assertGreater(len(modules), 1, 'test discovery found no sibling modules')
        # A clean environment: the child must not inherit a PYTHONPATH that would
        # hide a missing bootstrap, and must not reuse this process's sys.path.
        env = {k: v for k, v in os.environ.items() if k != 'PYTHONPATH'}
        failures = []
        for name in modules:
            done = subprocess.run(
                [sys.executable, '-c', CHILD, str(TESTS_DIR), name],
                capture_output=True, text=True, env=env, timeout=120)
            if done.returncode != 0:
                failures.append(f'{name}: {done.stderr.strip() or done.stdout.strip()}')
        self.assertEqual(
            [], failures,
            'these test modules do not import on their own — import _pipeline_path '
            'before any pipeline module:\n' + '\n'.join(failures))


if __name__ == '__main__':
    unittest.main()
