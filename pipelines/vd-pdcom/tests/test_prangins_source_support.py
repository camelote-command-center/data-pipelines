import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from prangins_source_support import compound

def ring(points):return [('l',a,b) for a,b in zip(points,points[1:]+points[:1])]
class WindingTests(unittest.TestCase):
 def test_nonzero_nested_same_direction(self):
  a=ring([(0,0),(10,0),(10,10),(0,10)]);b=ring([(2,2),(8,2),(8,8),(2,8)]);self.assertEqual(compound(a+b,False).area,100)
 def test_nonzero_opposite_hole(self):
  a=ring([(0,0),(10,0),(10,10),(0,10)]);b=ring([(2,2),(2,8),(8,8),(8,2)]);self.assertEqual(compound(a+b,False).area,64)
 def test_evenodd_same_direction_hole(self):
  a=ring([(0,0),(10,0),(10,10),(0,10)]);b=ring([(2,2),(8,2),(8,8),(2,8)]);self.assertEqual(compound(a+b,True).area,64)
 def test_invalid_ring_not_repaired(self):
  with self.assertRaises(ValueError):compound(ring([(0,0),(10,10),(0,10),(10,0)]),False)
 def test_disjoint_not_joined(self):
  a=ring([(0,0),(1,0),(1,1),(0,1)]);b=ring([(5,5),(6,5),(6,6),(5,6)]);g=compound(a+b,False);self.assertEqual(g.area,2);self.assertEqual(g.geom_type,'MultiPolygon')
if __name__=='__main__':unittest.main()
