import threading 

def consuming_enqueue(topic, client, message, index):
    topic.lock.acquire()
    next_expected = topic.next_index()
    if index < next_expected:
        topic.lock.release()
        return
    elif index > next_expected:
        try:
            messages = client.broker.consume(topic.name, next_expected)
            for m in messages:
                topic.messages.append(m)
        except e:
            topic.lock.release()
            return
        topic.lock.release()
        return
    else:
        topic.messages.append(message)
        topic.lock.release()
        return

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