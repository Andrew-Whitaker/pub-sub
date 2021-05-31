import unittest

from chord_node import *

class TestChordNode(unittest.TestCase):

    def test_ring_integrity(self):
        everyone = set()
        ring = create_chord_ring(['localhost:3001', 'localhost:3002', 'localhost:3003', 'localhost:3004', 'localhost:3005', 'localhost:3006'])
        for r in ring:
            succ = find_chord_successor(r.key, ring)
            everyone.add(succ[0].key)
        test = set([x.key for x in ring])
        self.assertTrue(len(test - everyone) == 0)
            
    # def test_upper(self):
    #     self.assertEqual('foo'.upper(), 'FOO')

    # def test_isupper(self):
    #     self.assertTrue('FOO'.isupper())
    #     self.assertFalse('Foo'.isupper())

    # def test_split(self):
    #     s = 'hello world'
    #     self.assertEqual(s.split(), ['hello', 'world'])
    #     # check that s.split fails when the separator is not a string
    #     with self.assertRaises(TypeError):
    #         s.split(2)

if __name__ == '__main__':
    unittest.main()