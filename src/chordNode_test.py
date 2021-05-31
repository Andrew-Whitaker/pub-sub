import unittest
from chordNode import *

class TestRingCreation(unittest.TestCase):
    def test_empty(self):
        ring = create_chord_ring([])
        self.assertEqual(len(ring), 0)

    def test_ordering(self):
        brokers = ["3001", "3002", "3003"]
        ring = create_chord_ring(brokers)
        self.assertLess(ring[0].hash_id, ring[1].hash_id)
        self.assertLess(ring[1].hash_id, ring[2].hash_id)

class TestFindPredecessor(unittest.TestCase):
    
    def test_find_single_node(self):
        brokers = ["3001"]
        ring = create_chord_ring(brokers)
        curr = ring[0]

        pred, index = find_chord_predecessor("any_string", ring)
        self.assertEqual(pred.hash_id, curr.hash_id)
        self.assertEqual(index, 0)

    def test_many_nodes(self):
        brokers = ["3001", "3002", "3003", "3004", "3005"]
        ring = create_chord_ring(brokers)
        for i in range(len(brokers)):
            curr = ring[i]
            pred, pi = find_chord_predecessor(curr.key, ring)
            if i > 0:
                self.assertLess(pred.hash_id, curr.hash_id)
                self.assertEqual(pi + 1, i)
            else:
                self.assertGreater(pred.hash_id, curr.hash_id)
                self.assertEqual(pi + 1, len(ring))

class TestFindSuccessor(unittest.TestCase):

    def test_find_single_node(self):
        brokers = ["3001"]
        ring = create_chord_ring(brokers)
        curr = ring[0]

        succ, index = find_chord_successor("any_string", ring)
        self.assertEqual(succ.hash_id, curr.hash_id)
        self.assertEqual(index, 0)

    def test_many_nodes(self):
        brokers = ["3001", "3002", "3003", "3004", "3005"]
        ring = create_chord_ring(brokers)
        for i in range(len(brokers)):
            curr = ring[i]
            succ, si = find_chord_successor(curr.key, ring)
            if i < len(ring) - 1:
                self.assertGreater(succ.hash_id, curr.hash_id)
                self.assertEqual(si - 1, i)
            else:
                self.assertLess(succ.hash_id, curr.hash_id)
                self.assertEqual(si, 0)


if __name__=='__main__':
    unittest.main()