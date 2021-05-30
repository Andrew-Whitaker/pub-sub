import sys

from zk_helpers import get_zookeeper_hosts
from pubsubClient import PubSubClient

class REPLEnv: 
    def __init__(self):
        self.topics = {}

    def set_index(self, topic, new_index): 
        self.topics[topic] = new_index

    def get_index(self, topic): 
        if topic not in self.topics:
            return 0
        return self.topics[topic]
        
def invoke(client, env, function, args):
    if function == "publish":
        if len(args) != 2:
            print("Error: publish expected 2 arguments, got {}".format(args)) 
            return False
        topic, message = args[0], args[1]
        client.publish(topic, message)
        if topic not in env.topics:
            env.set_index(topic, 0)
    if function == "consume":
        if len(args) != 2 and len(args) != 1:
            print("Error: usage 'consume' = consume <topic> <index?>") 
            return False
        topic = args[0]
        if len(args) == 2: 
            index = args[1]
        elif len(args) == 1: 
            index = env.get_index(topic)
        messages = client.consume(topic, int(index))
        env.set_index(topic, max(len(messages), env.get_index(topic)))
        print("\n".join(messages))

def main(zookeeper_addresses):
    env = REPLEnv()
    client = PubSubClient(zookeeper_addresses)
    while True:
        text = input("> ")
        inputs = text.split(" ")
        if inputs[0] == "q":
            return 0
        else:
            invoke(client, env, inputs[0].strip(), inputs[1:])
    
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python clientREPL.py <zk_config>") 
        exit(1)
    zk_config_path = sys.argv[1]
    zk_hosts = get_zookeeper_hosts(zk_config_path)
    print("CSE 223B PubSub REPL")
    main(zk_hosts)
