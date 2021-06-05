import sys
import threading
import time
import logging
import hashlib

from zk_helpers import get_zookeeper_hosts, makeHostsString
from pubsubClient import PubSubClient
from chord_node import *

class Consumer():

    def __init__(self, topics, psclient: PubSubClient) -> None:
        self.topics = topics        # List of topic names it will publish to
        self.psclient = psclient    # PubSubClient
        self.messages_consumed = {} # List of messages published by this publisher
        self.messages_digest = {}   # Hash digest of the messages published on this topic

    def run(self, timeout: int):
        # spawn a daemon thread for each topic provided
        for topic in self.topics:
            self.messages_consumed[topic] = 0
            self.messages_digest[topic] = hashlib.sha256()
            topic_thread = threading.Thread(target=self.consume_events, args=(topic,), daemon=True)
            topic_thread.start()
        time.sleep(timeout)
        return

    def consume_events(self, topic):
        msg_id = 0
        while True:
            messages = self.psclient.consume(topic, msg_id)
            for msg in messages:    
                self.messages_consumed[topic] += 1
                self.messages_digest[topic].update(msg.encode("utf-8"))
                msg_id += 1
            time.sleep(1)

    def get_logs(self):
        result = ""
        for topic in self.topics:
            result = result + "{}, {}, {}\n".format(topic, self.messages_consumed[topic], self.messages_digest[topic].hexdigest())
        return result

def run_consumer(i, topics, hosts, duration):
    print("Starting Consumer...")
    cons = Consumer(topics, PubSubClient(hosts))
    cons.run(duration)
    with open("tmp/output/consumer-{}.txt".format(i), "w") as f:
        f.write(cons.get_logs())

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python src/consumer.py <zk_config>") 
        exit(1)

    # ZooKeeper Config and PubSubClient
    i = 0
    zk_config_path = sys.argv[1]
    hosts = get_zookeeper_hosts(zk_config_path)
    duration = 60
    topics = ["alpha", "bravo", "charlie", "delta", "echo"]
    run_consumer(i, topics, hosts, duration)