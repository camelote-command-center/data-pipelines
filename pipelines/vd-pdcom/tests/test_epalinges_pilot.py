import sys
from pathlib import Path
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from epalinges_pilot import persist,SHA
class PilotGate(unittest.TestCase):
    def test_changed_source_never_replays_frozen_geometry(self):
        self.assertIsNone(persist(None,'8f42da46-9b59-5b72-aa5b-13522e418682','changed'))
    def test_unrelated_document_never_receives_geometry(self):
        self.assertIsNone(persist(None,'another-document',SHA))
