import sys

from pubsubClient import PubSubClient
from publisher import Publisher, run_publisher
from consumer import Consumer, run_consumer
from zk_helpers import get_zookeeper_hosts

import time
from multiprocessing import Process

def valid_duration_str(d):
    return d.replace('.','',1).isdigit()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python src/pub_sub_scale_test.py <zk_config> <no. publishers and consumers> <duration - seconds>") 
        exit(1)

    # ZooKeeper Config and PubSubClient
    zk_config_path = sys.argv[1]
    count = sys.argv[2]
    duration = sys.argv[3]

    if not count.isdigit() or not valid_duration_str(duration):
        print("Usage: python src/pub_sub_scale_test.py <zk_config> <no. publishers and consumers>") 
        exit(1)

    procs = []
    duration = float(duration)
    for i in range(0, int(count)):
        topics = ["a{}".format(i)]
        hosts  = get_zookeeper_hosts(zk_config_path)
        pubProcess = Process(target=run_publisher, args=(i, topics, hosts, duration, "logs/pubs/{}.txt".format(i)))
        pubProcess.start()
        procs.append(pubProcess)
        time.sleep(2)
        consProcess  = Process(target=run_consumer, args=(i, topics, hosts, duration, "logs/cons/{}.txt".format(i)))
        consProcess.start()
        procs.append(consProcess)

    [p.join() for p in procs]
    for i in range(0, int(count)):
        cons_log_file, pubs_log_file = open("logs/pubs/{}.txt".format(i)), open("logs/pubs/{}.txt".format(i))
        cons_logs, pubs_logs = cons_log_file.readlines(), pubs_log_file.readlines()
        for i, consumed_stream_data in enumerate(cons_logs):
            print("Checking '{}'=='{}'".format(consumed_stream_data, pubs_logs[i]))
            assert consumed_stream_data == pubs_logs[i]

    print("Done. Stream integrity was maintained.")

