import sys
import threading
import time

from zk_helpers import get_zookeeper_hosts, makeHostsString
from pubsubClient import PubSubClient
from chord_node import *

class Publisher():

    def __init__(self, myID: int, topics, psclient: PubSubClient) -> None:
        self.myID = myID             # ID that it will add to all messages
        self.topics = topics         # List of topic names it will publish to
        self.psclient = psclient     # PubSubClient 
        self.messages_published = {}  # List of messages published by this publisher
        self.messages_digest = {}    # Hash digest of the messages published on this topic

    def run(self, timeout: int):
        for topic in self.topics:
            self.messages_published[topic] = 0
            self.messages_digest[topic] = hashlib.sha256()
            topic_thread = threading.Thread(target=self.generate_events, args=(topic,), daemon=True)
            topic_thread.start()
        time.sleep(timeout)
        return

    def get_logs(self):
        result = ""
        for topic in self.topics:
            result = result + "{}, {}, {}\n".format(topic, self.messages_published[topic], self.messages_digest[topic].hexdigest())
        return result

    def generate_events(self, topic):
        msg_id = 0
        while True:
            message = "{}:{}".format(topic, str(msg_id))
            self.psclient.publish(topic, message)
            self.messages_published[topic] += 1
            self.messages_digest[topic].update(message.encode('utf-8'))
            msg_id += 1
            time.sleep(0.01)

def run_publisher(i, topics, hosts, duration, log_file):
    print("Starting Publisher...")
    pubs = Publisher(i, topics, PubSubClient(hosts))
    pubs.run(duration)
    if log_file is None:
        print(pubs.get_logs())
    else:
        with open(log_file, "w") as lf:
            lf.write(pubs.get_logs())

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python src/publisher.py <publisher ID> <zk_config>") 
        exit(1)

    # ZooKeeper Config and PubSubClient
    myID = int(sys.argv[1])
    zk_config_path = sys.argv[2]
    hosts = get_zookeeper_hosts(zk_config_path)
    topics = ["alpha", "bravo", "charlie"]
    duration = 60
    run_publisher(myID, topics, hosts, duration, None)

    
        

