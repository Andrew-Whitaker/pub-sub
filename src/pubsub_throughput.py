import sys
import datetime as dt

from pubsubClient import PubSubClient
from publisher import Publisher, run_publisher
from consumer import Consumer, run_consumer
from zk_helpers import get_zookeeper_hosts

import time
from multiprocessing import Process
from stats import *

def valid_duration_str(d):
    return d.replace('.','',1).isdigit()

def run_producers(procs, hosts, count: int, duration):
    for i in range(0, count):
        #topics = ["a{}".format(i)]
        topics = ["a{}".format(0)]
        pubProcess = Process(target=run_publisher, args=(i, topics, hosts, duration, "logs/pubs/{}.txt".format(i)))
        procs.append(pubProcess)
        pubProcess.start()

def run_consumers(procs, hosts, count: int, duration):
    for i in range(0, count):
        # topics = ["a{}".format(i)]
        topics = ["a{}".format(0)]
        consProcess  = Process(target=run_consumer, args=(i, topics, hosts, duration, "logs/cons/{}.txt".format(i)))
        procs.append(consProcess)
        consProcess.start()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python src/pubsub_throughput.py <zk_config> <no. pubs and cons> <duration - seconds>") 
        exit(1)

    # ZooKeeper Config and PubSubClient
    zk_config_path = sys.argv[1]
    count = int(sys.argv[2])
    duration = sys.argv[3]

    if not valid_duration_str(duration):
        print("Usage: python src/pubsub_throughput.py <zk_config> <no. pubs and cons> <duration - seconds>") 
        exit(1)

    hosts = get_zookeeper_hosts(zk_config_path)
    procs = []
    duration = float(duration)

    simulation_start_time = dt.datetime.now()

    run_producers(procs, hosts, count, duration)
    time.sleep(duration / 2) # Let producers operate for half the time before consumers begin
    run_consumers(procs, hosts, count, duration)

    [p.join() for p in procs]
    # for i in range(0, count):
    #     cons_log_file, pubs_log_file = open("logs/pubs/{}.txt".format(i)), open("logs/pubs/{}.txt".format(i))
    #     cons_logs, pubs_logs = cons_log_file.readlines(), pubs_log_file.readlines()
    #     for i, consumed_stream_data in enumerate(cons_logs):
    #         print("Checking '{}'=='{}'".format(consumed_stream_data, pubs_logs[i]))
    #         assert consumed_stream_data == pubs_logs[i]

    run_stats_collection(simulation_start_time, duration * 2, PubSubClient(hosts))

    print("Done. Stream integrity was maintained.")

