import sys

from zk_helpers import get_zookeeper_hosts
from pubsubClient import PubSubClient

# ===== REPL ENVIRONMENT =====
class REPLEnv: 
    def __init__(self):
        self.target = None
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

def parse_args(string):
    remaining = string.strip()
    quotes_indices = [pos for pos, char in enumerate(remaining) if char == '"']
    if len(quotes_indices) % 2 != 0:
        raise Exception("Invalid use of quotations")
    pairs = [(quotes_indices[i]+1, quotes_indices[i+1]) for i in range(0, len(quotes_indices), 2)]
    start = 0
    args = []
    for i, p in enumerate(pairs):
        if p[0] - start > 0:
            [args.append(x.strip()) for x in remaining[start:p[0]-1].split(" ") if x != ""]
        args.append(remaining[p[0]:p[1]])
        if i == len(pairs)-1 and len(remaining) - p[1] > 0:
            [args.append(x.strip()) for x in remaining[p[1]+1:len(remaining)].split(" ") if x != ""]
        start = p[1] + 1
    return args

# TODO Clean up the command parser, do a little more testing verify
def parse_line(line): 
    tokens = parse_args(line)
    cmd = tokens[0]
    if cmd == "publish": 
        if len(tokens) != 3:
            raise Exception("usage: publish <topic> <message>")
        topic, message = tokens[1], tokens[2]
        return Publish(topic.strip(), message.strip())
    elif cmd == "consume":
        if len(tokens) == 2:
            topic, index = remaining.strip(), None
        elif len(tokens) == 3:
            topic, index = tokens[1], tokens[2]
            if not index.isdigit():
                raise Exception("usage: publish <topic> <message>")
        else:
            
        return Consume(topic, index)
    elif cmd == "brokers":
        return Brokers()
    elif cmd == "primary":
        topic = remaining.strip().split(" ", 1)[0]
        return Primary(topic.strip())
    elif cmd == "target":
        address = remaining.strip()
        return Target(address) 
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
        return True
    if isinstance(action, Quit): 
        if env.single_node_mode():
            env.target = None
            return True
        return False
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

# ========= END REPL AST =========

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python src/client_repl.py <zk_config>") 
        exit(1)
    zk_config_path = sys.argv[1]
    zk_hosts = get_zookeeper_hosts(zk_config_path)
    print("CSE 223B PubSub REPL")
    main(zk_hosts)
