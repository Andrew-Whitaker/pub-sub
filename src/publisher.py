import sys
import threading
import time

from pubsubClient import PubSubClient
from chord_node import *
from repeating_timer import RepeatingTimer
from zk_helpers import get_zookeeper_hosts, makeHostsString

class Publisher():

    def __init__(self, myID: int, topics, psclient: PubSubClient) -> None:
        self.myID = myID             # ID that it will add to all messages
        self.topics = topics         # List of topic names it will publish to
        self.psclient = psclient     # PubSubClient 
        self.time = 0                # Time used for reporting statistics
        self.messages = {}           # List of messages published by this publisher
        self.messages_published = {} # Number of messages published
        self.last_messages_cnt = 0  # 
        self.messages_digest = {}    # Hash digest of the messages published on this topic

    def run(self, timeout: int):
        def report_statistics():
            for topic in self.topics: 
                self.time += 1
                current_total_messages = self.messages_published[topic]
                m_count = current_total_messages - self.last_messages_cnt
                message = "{}, {}, {}".format(self.time, m_count, float(current_total_messages)/float(self.time))
                self.psclient.publish(topic + "-meta-publish", message)
                self.last_messages_cnt = current_total_messages
        e = threading.Event()
        self.timer = RepeatingTimer(e)
        self.timer.start(1, report_statistics)
        threads = []
        for topic in self.topics:
            self.messages[topic] = []
            self.messages_published[topic] = 0
            self.messages_digest[topic] = hashlib.sha256()
            topic_thread = threading.Thread(target=self.generate_events, args=(topic, timeout), daemon=True)
            topic_thread.start()
            threads.append(topic_thread)
        [t.join() for t in threads]
        self.timer.stop()
        return

    def get_logs(self):
        result = ""
        for topic in self.topics:
            result = result + "{}, {}, {}\n".format(topic, self.messages_published[topic], self.messages_digest[topic].hexdigest())
            result += "\n".join(["{}: {}".format(i, x) for i, x in enumerate(self.messages[topic])])
        return result

    def generate_events(self, topic, timeout):
        elapsed = 0.0
        msg_id = 0
        while True:
            start = time.time()
            message = "{}:{}".format(topic, str(msg_id))
            self.psclient.publish(topic, message)
            self.messages[topic].append(message)
            self.messages_published[topic] += 1
            self.messages_digest[topic].update(message.encode('utf-8'))
            msg_id += 1
            time.sleep(0.01)
            elapsed += time.time() - start
            if elapsed > timeout:
                break

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

    
        

