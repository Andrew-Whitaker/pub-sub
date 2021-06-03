import sys
import threading
import time

from zk_helpers import get_zookeeper_hosts, makeHostsString
from pubsubClient import PubSubClient
from chord_node import *


class Publisher():

    def __init__(self, myID: int, topics, psclient: PubSubClient) -> None:
        self.myID = myID         # ID that it will add to all messages
        self.topics = topics     # List of topic names it will publish to
        self.psclient = psclient # PubSubClient

    def run(self, timeout: int):

        # spawn a daemon thread for each topic provided
        for topic in self.topics:
            # Daemon threads will be terminated when the main thread terminates
            topic_thread = threading.Thread(target=self.generate_events, args=(topic), daemon=True)
            topic_thread.start()

        time.sleep(timeout)
        return

    def generate_events(self, topic):
        msg_id = 0
        while True:
            # create message & publish
            message = "T:{} - I:{} - M:{}".format(topic, str(self.myID), str(msg_id))
            self.psclient.publish(topic, message)
            time.sleep(0.01)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python src/publisher.py <publisher ID> <zk_config>") 
        exit(1)

    print("Starting Publisher...")

    # ZooKeeper Config and PubSubClient
    myID = sys.argv[1]
    zk_config_path = sys.argv[2]
    topics = ["alpha", "bravo", "charlie", "delta", "echo"]
    pub = Publisher(myID, topics, PubSubClient(get_zookeeper_hosts(zk_config_path)))
    
    # Run Publisher for 1 minute
    pub.run(60)

    
        

