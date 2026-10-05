"""Every test module that imports a pipeline module must put the path there itself.

Why this test exists: unittest discovery imports test modules in filename order
and puts only this directory on sys.path. A module that imports a pipeline module
by bare name without adding the pipeline directory still passes as long as some
module loaded earlier happened to add it — until a file is renamed or a new one
sorts first. On 2026-10-05 that is precisely what took down the sibling pipeline
ch-planning-documents: four test modules whose names sorted before the module
holding the sys.path.insert failed with ModuleNotFoundError, the "Verify parser
contracts" step went red, acquisition was skipped for the national scope and the
founder got "Parser ch_planning_document_text en erreur" (run 37331861426).

Here that would stop vd-pdcom acquisition the same way. This test reads the
imports statically, so it costs nothing, and fails naming any module that relies
on another module's import side effect.
"""
import ast
import pathlib
import unittest

TESTS_DIR = pathlib.Path(__file__).resolve().parent
PIPELINE_DIR = TESTS_DIR.parent
LOCAL_MODULES = {p.stem for p in PIPELINE_DIR.glob('*.py')}


def _local_imports(tree):
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names = [a.name.split('.')[0] for a in node.names]
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            names = [node.module.split('.')[0]]
        else:
            continue
        found |= {n for n in names if n in LOCAL_MODULES}
    return found


class PathBootstrapTests(unittest.TestCase):
    def test_pipeline_imports_carry_their_own_path(self):
        offenders = []
        for path in sorted(TESTS_DIR.glob('test_*.py')):
            source = path.read_text()
            imported = _local_imports(ast.parse(source))
            if not imported:
                continue
            if 'sys.path.insert' in source or '_pipeline_path' in source:
                continue
            offenders.append(f'{path.name} imports {", ".join(sorted(imported))}')
        self.assertEqual(
            [], offenders,
            'these test modules import a pipeline module but never put the pipeline '
            'directory on sys.path, so they only work when another test module is '
            'loaded first — add `import _pipeline_path` above the pipeline imports:\n'
            + '\n'.join(offenders))


if __name__ == '__main__':
    unittest.main()
