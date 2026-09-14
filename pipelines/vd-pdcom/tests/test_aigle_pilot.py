import sys
from pathlib import Path
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from aigle_pilot import persist,SHA
class PilotGate(unittest.TestCase):
    def test_changed_source_never_replays_frozen_geometry(self):
        self.assertIsNone(persist(None,'dd172ee9-e928-5c2f-99d6-3a24eb28cd68','changed'))
    def test_unrelated_document_never_receives_geometry(self):
        self.assertIsNone(persist(None,'another-document',SHA))
