import unittest,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orbe_pilot import tiles
class OrbeTiles(unittest.TestCase):
 def test_two_tiles_preserve_orbe_extent(self):
  b=[0,0,1294.8,2158.5];t=tiles(b)
  self.assertEqual(len(t),2);self.assertEqual(t[0][3],t[1][1]);self.assertEqual(t[0][:2],b[:2]);self.assertEqual(t[-1][2:],b[2:])
  self.assertAlmostEqual(sum((x1-x0)*(y1-y0) for x0,y0,x1,y1 in t),b[2]*b[3])
  self.assertTrue(all(0<x1-x0<=2000 and 0<y1-y0<=2000 for x0,y0,x1,y1 in t))
 def test_exact_limit(self):self.assertEqual(tiles([0,0,2000,2000]),[[0,0,2000,2000]])
 def test_reject_unbounded_or_invalid(self):
  for b in [[0,0,4001,1],[0,0,1,float('inf')],[0,0,0,1],[0,0,1]]:
   with self.assertRaises(ValueError):tiles(b)
