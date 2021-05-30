import sys
import os
import time

from zk_helpers import get_zookeeper_hosts
from pubsubBroker import start_broker
from pubsubClient import PubSubClient
from multiprocessing import Process

def setup_brokers(zk, brokers):
    b_processes = []
    for broker in range(1, brokers + 1):  
        print("Starting Broker {}".format(broker))
        broker_config_path = "./tmp/broker/{}.txt".format(broker)
        exists = os.path.isfile(broker_config_path) 
        if exists:
            with open(broker_config_path, "r") as f:
                broker_conf_array = f.readlines()
                url = broker_conf_array[0].strip()
        p = Process(target=start_broker, args=(zk_config_path, url))
        b_processes.append(p)
        
        p.start()
        time.sleep(4)
        
    # verify that they are running
    while True:
        u_input = input("> ")
        if u_input.strip() == "q":
            [p.terminate() for p in b_processes] 
            return

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python src/pubsub_integ_test.py <zk_config> <broker_count>") 
        exit(1)
    zk_config_path = sys.argv[1]
    zk_hosts = get_zookeeper_hosts(zk_config_path)
    if not sys.argv[2].isdigit():
        print("Usage: python src/pubsub_integ_test.py <zk_config> <broker_count>") 
        exit(1)    
    broker_count = int(sys.argv[2])
    setup_brokers(zk_hosts, broker_count)