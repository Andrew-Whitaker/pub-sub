import sys

from zk_helpers import get_zookeeper_hosts
from pubsubClient import PubSubClient

# ========= REPL AST =========
class Consume: 
    def __init__(self, topic, index):
        self.topic = topic
        self.index = index
    def __str__(self):
        return "Consume(topic={}, index={})".format(self.topic, self.index)

class Publish: 
    def __init__(self, topic, message):
        self.topic = topic
        self.message = message
    def __str__(self):
        return "Publish(topic={}, message={})".format(self.topic, self.message)

class Quit: 
    def __init__(self):
        pass
    def __str__(self):
        return "Quit()"

# ========= END REPL AST =========

def parse_line(line): 
    remaining = line.strip()
    boxed = remaining.split(" ", 1)
    if len(boxed) == 1:
        cmd = boxed[0]
        remaining = ""
    else:
        cmd, remaining = [x.strip() for x in remaining.split(" ", 1)]
    
    if cmd == "publish": 
        topic, message = remaining.strip().split(" ", 1)
        return Publish(topic.strip(), message.strip())
    elif cmd == "consume":
        if " " in remaining:
            topic, index = remaining.strip().split(" ", 1)
            topic, index = topic.strip(), index.strip()
            if not index.isdigit():
                raise Exception("consume index must be a number")
            index = int(index)
        else:
            topic = remaining.strip()
            index = None
        return Consume(topic, index)
    elif cmd == "q":
        return Quit()
    else: 
        raise Exception("parse error") 

class REPLEnv: 
    def __init__(self):
        self.topics = {}

    def set_index(self, topic, new_index): 
        self.topics[topic] = new_index

    def get_index(self, topic): 
        if topic not in self.topics:
            return 0
        return self.topics[topic]
        
def perform(client, env, action):
    if isinstance(action, Publish):
        if client.publish(action.topic, action.message): 
            print("{} Succeeded.".format(action))
        else:
            print("{} Failed.".format(action))
        return True
    if isinstance(action, Consume): 
        index = env.get_index(action.topic) if action.index is None else action.index
        messages = client.consume(action.topic, index)
        new_index = max(index + len(messages), env.get_index(action.topic))
        env.set_index(action.topic, new_index)
        print("\n".join(messages))
        return True
    if isinstance(action, Quit): 
        return False

def main(zookeeper_addresses):
    env = REPLEnv()
    client = PubSubClient(zookeeper_addresses)
    while True:
        user_input = input("> ")
        action = parse_line(user_input)
        if not perform(client, env, action):
            return 
    
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python src/client_repl.py <zk_config>") 
        exit(1)
    zk_config_path = sys.argv[1]
    zk_hosts = get_zookeeper_hosts(zk_config_path)
    print("CSE 223B PubSub REPL")
    main(zk_hosts)
