import unittest

from topic import OutOfOrderBuffer, Topic, consuming_enqueue

class _Broker:
    def __init__(self,  topic: str, topic_messages):        
        self.topic_messages = topic_messages
    def consume(self, topic, index):
        return self.topic_messages[index:]

class MockBrokerRPCClient:
    def __init__(self,  topic: str, topic_messages):        
        self.broker = _Broker(topic, topic_messages)
    
class TestConsumingEnqueue(unittest.TestCase):
    def test_basic_addition(self):
        t = Topic("balloons")
        c = MockBrokerRPCClient("topic", [])
        consuming_enqueue(t, c, "Message0", 0)
        consuming_enqueue(t, c, "Message1", 1)
        consuming_enqueue(t, c, "Message2", 2)
        self.assertEqual(t.next_index(), 3)
        self.assertEqual(t.consume(0), ["Message0","Message1","Message2"])
    
    def test_basic_ooo_addition_1(self):
        t = Topic("balloons")
        c = MockBrokerRPCClient("topic", ["Message0", "Message1"])
        consuming_enqueue(t, c, "Message1", 1)
        consuming_enqueue(t, c, "Message2", 2)
        self.assertEqual(t.next_index(), 3)
        self.assertEqual(t.consume(0), ["Message0","Message1","Message2"])
    
    def test_basic_ooo_addition_2(self):
        t = Topic("balloons")
        c = MockBrokerRPCClient("topic", ["Message0", "Message1"])
        consuming_enqueue(t, c, "Message1", 1)
        consuming_enqueue(t, c, "Message2", 2)
        consuming_enqueue(t, c, "Message1", 1)
        self.assertEqual(t.next_index(), 3)
        self.assertEqual(t.consume(0), ["Message0","Message1","Message2"])
    
if __name__ == '__main__':
    unittest.main()
