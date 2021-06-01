import threading 

def consuming_enqueue(topic, client, message, index):
    next_expected = topic.next_index()
    if index < next_expected:
        return
    elif index > next_expected:
        messages = client.broker.consume(topic, next_expected)
        for m in messages:
            topic.publish(m)
    else:
        topic.publish(message)
    return 
        
class Topic: 
    def __init__(self, name):
        self.lock = threading.Lock()
        self.message_count = 0
        self.messages = []
        
    def publish(self, message):
        self.lock.acquire()
        self.messages.append(message)
        result = self.message_count
        self.message_count += 1
        self.lock.release()
        return result 
    
    def consume(self, index):
        if index < 0: 
            index = 0
        elif index >= len(self.messages):
            return []
        return self.messages[index:]
    
    def next_index(self): 
        return self.message_count