import threading 

def indexed_enqueue(topic, ooo_buffer, message, index):
    next_expected = topic.next_index()
    if ooo_buffer.has(next_expected, index):
        [topic.publish(m) for m in ooo_buffer.extract(next_expected, index)]
    if index == topic.next_index(): 
        topic.publish(message)
    elif index > topic.next_index():            
        ooo_buffer.publish(message, index)
    latest_known_message = ooo_buffer.max()
    if latest_known_message[1] < 0:
        return 
    if ooo_buffer.has(topic.next_index(), latest_known_message[1] + 1):
        [topic.publish(m) for m in ooo_buffer.extract(next_expected, latest_known_message[1] + 1)]

def isComplete(l):
    return sorted(l) == list(range(min(l), max(l)+1))

class OutOfOrderBuffer: 
    def __init__(self, name):
        self.lock = threading.Lock()
        self.length = 0
        self.messages = []

    def __len__(self): 
        return self.length
        
    def publish(self, message, desired_index):
        self.lock.acquire()
        self.messages.append((message, desired_index))
        self.length += 1
        self.lock.release()
    
    def indices(self):
        self.lock.acquire()
        result = [x[1] for x in self.messages]
        result.sort()
        self.lock.release()
        return result
    
    def has(self, start, end):
        self.lock.acquire()
        values = [start - 1] + [x[1] for x in self.messages] + [end]
        self.lock.release()
        return isComplete(values)
    
    def max(self):
        self.lock.acquire()
        x = ('', -1)
        if len(self.messages) > 0:
            x = max(self.messages, key=lambda x:x[1])
        self.lock.release()
        return x

    def extract(self, start, end):
        self.lock.acquire()
        results = []
        new_messages = []
        for i, message in enumerate(self.messages):
            if message[1] in range(start, end):
                results.append(message)
            else:
                new_messages.append(message)
        self.messages = new_messages
        self.lock.release()
        results.sort(key = lambda x: x[1])
        return results
        
class Topic: 
    def __init__(self, name):
        self.lock = threading.Lock()
        self.message_count = 0
        self.messages = []
        
    def publish(self, message):
        self.lock.acquire()
        self.messages.append(message)
        self.message_count += 1
        self.lock.release()
    
    def consume(self, index):
        if index < 0: 
            index = 0
        elif index >= len(self.messages):
            return []
        return self.messages[index:]
    
    def next_index(self): 
        return self.message_count