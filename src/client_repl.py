import sys

from zk_helpers import get_zookeeper_hosts
from pubsubClient import PubSubClient, buildBrokerClient


# ===== REPL ENVIRONMENT =====
class REPLEnv: 
    def __init__(self):
        self.target = None
        self.broker_rpc_client = None
        self.topics = {}

    def single_node_mode(self):
        return self.target != None
        
    def set_index(self, topic, new_index): 
        self.topics[topic] = new_index
    
    def get_index(self, topic): 
        if topic not in self.topics:
            return 0
        return self.topics[topic]

# ===== END REPL ENVIRONMENT =====

# TODO fix this shit code
def parse_args(string):
    remaining = string.strip()
    if '"' not in remaining:
        return [x.strip() for x in remaining.split(" ")]
    quotes_indices = [pos for pos, char in enumerate(remaining) if char == '"']
    if len(quotes_indices) % 2 != 0:
        raise Exception("Invalid use of quotations")
    pairs = [(quotes_indices[i]+1, quotes_indices[i+1]) for i in range(0, len(quotes_indices), 2)]
    start, args = 0, []
    for i, p in enumerate(pairs):
        if p[0] - start > 0:
            [args.append(x.strip()) for x in remaining[start:p[0]-1].split(" ") if x != ""]
        args.append(remaining[p[0]:p[1]])
        if i == len(pairs)-1 and len(remaining) - p[1] > 0:
            [args.append(x.strip()) for x in remaining[p[1]+1:len(remaining)].split(" ") if x != ""]
        start = p[1] + 1
    return args

def parse_line(line): 
    tokens = parse_args(line)
    if len(tokens) < 1:
        return
    cmd = tokens[0]
    if cmd == "publish": 
        if len(tokens) != 3:
            raise Exception("usage: publish <topic> <message>")
        topic, message = tokens[1], tokens[2]
        return Publish(topic.strip(), message.strip())
    elif cmd == "consume":
        if len(tokens) == 2:
            topic, index = tokens[1], None
        elif len(tokens) == 3:
            topic, index = tokens[1], tokens[2]
            if not index.isdigit():
                print("Erro: usage: publish <topic> <message>")
                return None
        else:
            print("Error: usage: publish <topic> <message>")
            return None
        index = int(index) if index is not None else None
        return Consume(topic, index)
    elif cmd == "brokers":
        return Brokers()
    elif cmd == "primary":
        if len(tokens) != 2:
            print("Error: usage: primary <topic>")
            return None 
        topic = tokens[1]
        return Primary(topic.strip())
    elif cmd == "target":
        if len(tokens) != 2:
            print("usage: target <address>")      
            return None 
        address = tokens[1]
        return Target(address) 
    elif cmd == "get_topics":
        if len(tokens) != 1:
            print("Error usage: get_topics")      
            return None 
        return GetTopics() 
    elif cmd == "get_queue":
        if len(tokens) != 2:
            print("usage (in target): get_queue <topic>")      
            return None 
        topic = tokens[1]
        return GetQueue(topic) 
    elif cmd == "q":
        return Quit()
        
def perform(client, env, action):
    if isinstance(action, Publish):
        if client.publish(action.topic, action.message): 
            print("{} succeeded.".format(action))
        else:
            print("{} failed.".format(action))
        return True
    if isinstance(action, Consume): 
        index = env.get_index(action.topic) if action.index is None else action.index
        messages = client.consume(action.topic, index)
        new_index = max(index + len(messages), env.get_index(action.topic))
        env.set_index(action.topic, new_index)
        print("\n".join(messages))
        return True
    if isinstance(action, Brokers): 
        brokers = client.fetch_brokers()
        print("\n".join([str(x) for x in brokers]))
        return True
    if isinstance(action, Primary): 
        primary = client.primary(action.topic)
        print(primary[0])
        return True
    if isinstance(action, Target):
        env.target = action.address
        env.broker_rpc_client = buildBrokerClient(env.target)
        return True
    if isinstance(action, Quit): 
        if env.single_node_mode():
            env.target = None
            env.broker_rpc_client = None
            return True
        return False
    if isinstance(action, GetTopics): 
        if not env.single_node_mode():
            print("Error: must target a single broker")
            return True
        topics = env.broker_rpc_client.broker.get_topics()
        print("\n".join(topics))
        return True
    if isinstance(action, GetQueue): 
        if not env.single_node_mode():
            print("Error: must target a single broker")
            return True
        print(env.broker_rpc_client.broker.get_queue(action.topic))
        return True
    return True

def main(zookeeper_addresses):
    env = REPLEnv()
    client = PubSubClient(zookeeper_addresses)
    while True:
        prompt = " ({}) > ".format(env.target) if env.single_node_mode() else " > "
        user_input = input(prompt)
        action = parse_line(user_input)
        if not perform(client, env, action):
            return 

# ========= REPL AST =========
class Consume: 
    def __init__(self, topic, index):
        self.topic = topic
        self.index = index
    def __str__(self):
        return "Consume(topic={}, index={})".format(self.topic, self.index)
    def __eq__(self, obj):
        return isinstance(obj, Consume) and obj.topic == self.topic and obj.index == self.index

class Publish: 
    def __init__(self, topic, message):
        self.topic = topic
        self.message = message
    def __str__(self):
        return "Publish(topic={}, message={})".format(self.topic, self.message)
    def __eq__(self, obj):
        return isinstance(obj, Publish) and obj.topic == self.topic and obj.message == self.message

class Target: 
    def __init__(self, address):
        self.address = address
    def __str__(self):
        return "Target({})".format(self.address)
    def __eq__(self, obj):
        return isinstance(obj, Target) and obj.address == self.address

class Quit: 
    def __init__(self):
        pass
    def __str__(self):
        return "Quit()"
    def __eq__(self, obj):
        return isinstance(obj, Quit)

class Brokers: 
    def __init__(self):
        pass
    def __str__(self):
        return "Brokers()"
    def __eq__(self, obj):
        return isinstance(obj, Brokers)
class Primary: 
    def __init__(self, topic):
        self.topic = topic
    def __str__(self):
        return "Primary()"
    def __eq__(self, obj):
        return isinstance(obj, Primary) and self.topic == obj.topic

class GetTopics: 
    def __init__(self):
        pass
    def __str__(self):
        return "GetTopics()"
    def __eq__(self, obj):
        return isinstance(obj, GetTopics)

class GetQueue: 
    def __init__(self, topic):
        self.topic = topic
    def __str__(self):
        return "GetQueue()"
    def __eq__(self, obj):
        return isinstance(obj, GetQueue) and self.topic == obj.topic

# ========= END REPL AST =========

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python src/client_repl.py <host1> <host2> ...") 
        exit(1)
    zk_hosts = sys.argv[1:]
    main(zk_hosts)
