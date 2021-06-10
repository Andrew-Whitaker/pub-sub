import sys
import datetime as dt

from pubsubClient import PubSubClient
from publisher import Publisher, run_publisher
from consumer import Consumer, run_consumer
from zk_helpers import get_zookeeper_hosts

import time
from multiprocessing import Process
from stats import *


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python src/pubsub_throughput.py <duration (s)> <zk hosts ....>") 
        exit(1)

    # ZooKeeper Config and PubSubClient
    duration = sys.argv[1]
    zk_hosts = sys.argv[2:]

    duration = float(duration)

    simulation_start_time = dt.datetime.utcnow()
    run_stats_collection(simulation_start_time, duration * 1.5, PubSubClient(zk_hosts))

    print("Done. Stream integrity was maintained.")

