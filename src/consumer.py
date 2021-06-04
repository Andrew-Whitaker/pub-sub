import sys
import threading
import time
import logging

from zk_helpers import get_zookeeper_hosts, makeHostsString
from pubsubClient import PubSubClient
from chord_node import *


class Consumer():

    def __init__(self, myID: int, topics, psclient: PubSubClient) -> None:
        self.myID = myID         # ID that it will add to all messages
        self.topics = topics     # List of topic names it will publish to
        self.psclient = psclient # PubSubClient

    def run(self, timeout: int):

        # spawn a daemon thread for each topic provided
        for topic in self.topics:
            # Daemon threads will be terminated when the main thread terminates
            topic_thread = threading.Thread(target=self.consume_events, args=(topic), daemon=True)
            topic_thread.start()

        time.sleep(timeout)
        return

    def consume_events(self, topic):
        msg_id = 0
        while True:
            # create message & publish
            messages = self.psclient.consume(topic, msg_id)
            for msg in messages:
                logging.warning(msg)
                msg_id += 1
            time.sleep(0.01)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python src/consumer.py <zk_config>") 
        exit(1)

    print("Starting Publisher...")

    # ZooKeeper Config and PubSubClient
    zk_config_path = sys.argv[1]
    topics = ["alpha", "bravo", "charlie", "delta", "echo"]
    consumer = Consumer(topics, PubSubClient(get_zookeeper_hosts(zk_config_path)))
    
    # Run Publisher for 1 minute
    consumer.run(60)