import threading 

def consuming_enqueue(topic, client, message, index):
    next_expected = topic.next_index()
    if index < next_expected:
        return
    elif index > next_expected:
        messages = client.broker.consume(topic.name, next_expected)
        topic.lock.acquire()
        expected_messages = (index - topic.topic.next_index()) + 1
        if expected_messages == len(messages):
            for m in messages:
                topic.messages.append(message)
        topic.lock.release()

    else:
        topic.lock.acquire()
        if topic.next_index() == index:
            topic.publish(message)    
        topic.lock.release()

class Topic: 
    def __init__(self, name):
        self.lock = threading.Lock()
        self.messages = []
        self.name = name
        
    def publish(self, message):
        self.lock.acquire()
        self.messages.append(message)
        result = len(self.messages)
        self.lock.release()
        return result 
    
    def consume(self, index):
        if index < 0: 
            index = 0
        elif index >= len(self.messages):
            return []
        return self.messages[index:]
    
    def next_index(self): 
        return len(self.messages)

    def __str__(self):
        return str(self.messages)