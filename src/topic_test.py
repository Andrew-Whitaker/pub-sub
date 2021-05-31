import unittest

from topic import OutOfOrderBuffer, Topic, indexed_enqueue

class TestOutOfOrderBuffer(unittest.TestCase):
 
    def test_OOB_correct_indices(self):
        x = OutOfOrderBuffer("balloons")
        x.publish("Message", 9)
        x.publish("Message2", 4)
        x.publish("Message3", 0)
        self.assertEqual(x.indices(), [0, 4, 9])
        self.assertEqual(x.max(), ('Message', 9))

    def test_OOB_correct_lengths(self):
        x = OutOfOrderBuffer("balloons")
        self.assertEqual(len(x), 0)
        x.publish("Message", 9)
        x.publish("Message2", 4)
        x.publish("Message3", 0)
        self.assertEqual(len(x), 3)
    
    def test_OOB_correct_has_one(self):
        x = OutOfOrderBuffer("balloons")
        self.assertEqual(len(x), 0)
        x.publish("Message2", 2)
        self.assertFalse(x.has(1, 3))
        x.publish("Message1", 1)
        self.assertTrue(x.has(1, 3))

    def test_OOB_correct_extract(self):
        x = OutOfOrderBuffer("balloons")
        x.publish("Message3", 3)
        x.publish("Message2", 2)
        x.publish("Message1", 1)
        self.assertEqual(x.indices(), [1, 2, 3])
        res = x.extract(1, 4)
        self.assertEqual(x.indices(), [])
        self.assertEqual(res, [('Message1', 1), ('Message2', 2), ('Message3', 3)])

    def test_basic_indexed_enqueue_one(self):
        t = Topic("balloons")
        oob = OutOfOrderBuffer("balloons")
        indexed_enqueue(t, oob, "message_0", 0)
        indexed_enqueue(t, oob, "message_1", 1)
        self.assertEqual(t.next_index(), 2)

    def test_basic_indexed_enqueue_two(self):
        t = Topic("balloons")
        oob = OutOfOrderBuffer("balloons")
        indexed_enqueue(t, oob, "message_0", 0)
        indexed_enqueue(t, oob, "message_2", 2)
        self.assertEqual(t.next_index(), 1)
        indexed_enqueue(t, oob, "message_1", 1)
        self.assertEqual(t.next_index(), 3)
        indexed_enqueue(t, oob, "message_3", 3)
        self.assertEqual(t.next_index(), 4)
    
    def test_basic_indexed_enqueue_three(self):
        t = Topic("balloons")
        oob = OutOfOrderBuffer("balloons")
        indexed_enqueue(t, oob, "message_3", 3)
        self.assertEqual(t.next_index(), 0)
        self.assertEqual(len(oob), 1)
        indexed_enqueue(t, oob, "message_2", 2)
        self.assertEqual(t.next_index(), 0)
        self.assertEqual(len(oob), 2)
        indexed_enqueue(t, oob, "message_1", 1)  
        self.assertEqual(t.next_index(), 0)
        self.assertEqual(len(oob), 3)
        indexed_enqueue(t, oob, "message_0", 0)  
        self.assertEqual(t.next_index(), 4)
    
if __name__ == '__main__':
    unittest.main()