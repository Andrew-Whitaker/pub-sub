import sys
import threading
import time
import datetime as dt
import json

from pubsubClient import PubSubClient
from chord_node import *
from stats import *
from repeating_timer import RepeatingTimer
from zk_helpers import get_zookeeper_hosts, makeHostsString

STATS_TIME_FRAME = 1 # seconds
STATS_TOPIC = "SYSTEM_STATS"

class Publisher():

    def __init__(self, myID: int, topics, psclient: PubSubClient) -> None:
        self.myID = myID             # ID that it will add to all messages
        self.topics = topics         # List of topic names it will publish to
        self.psclient = psclient     # PubSubClient 
        self.messages = {}           # List of messages published by this publisher
        self.messages_published = {} # Number of messages published
        self.last_messages_cnt = {}  # 
        self.messages_digest = {}    # Hash digest of the messages published on this topic

    def run(self, timeout: int):
        def report_statistics():
            for topic in self.topics: 
                timestamp = dt.datetime.now()
                current_total_messages = self.messages_published[topic]
                msg_count = current_total_messages - self.last_messages_cnt[topic]
                start_str = encode_datetime(timestamp - dt.timedelta(seconds=1))
                end_str = encode_datetime(timestamp)
                stat_event = ThroughputStat(start_str, end_str, msg_count, topic)
                self.psclient.publish(STATS_TOPIC, json.dumps(stat_event))
                self.last_messages_cnt[topic] = current_total_messages
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

    # No Timestamps
    # def generate_events(self, topic):
    #     msg_id = 0
    #     time_stamp = 0  
    #     while True:
    #         start_time = dt.datetime.now()
    #         end_frame_time = start_time + dt.timedelta(seconds=1)
    #         previous_msg_count = 0
    #         while end_frame_time < dt.datetime.now():
    #             message = "{}:{}".format(topic, str(msg_id))
    #             self.psclient.publish(topic, message)
    #             self.messages_published[topic] += 1
    #             self.messages_digest[topic].update(message.encode('utf-8'))
    #             msg_id += 1
    #         stat_event = ThroughputStat(time_stamp, time_stamp+1, self.messages_published[topic] - previous_msg_count, topic)
    #         self.psclient.publish(STATS__TOPIC, json.dumps(stat_event))

    def generate_events(self, topic, timeout):
        elapsed = 0.0
        msg_id = 0
        time_stamp = 0  
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

    
        

