import sys
import time
from multiprocessing import Process
from publisher import run_publisher
from stats import *

def run_producers(procs, count: int, duration, hosts, topics):
    for i in range(0, count):
        pubProcess = Process(target=run_publisher, args=(i, topics, hosts, duration))
        procs.append(pubProcess)
        pubProcess.start()

if __name__ == '__main__':
    if len(sys.argv) < 5:
        print("Usage: python src/publishers.py <no. pubs> <duration (s)> <zk hosts...> <topics...>") 
        exit(1)

    count = int(sys.argv[1])
    duration = float(sys.argv[2])
    hosts = []
    topics = []
    for i in range(3, len(sys.argv)):
        arg = sys.argv[i]
        if ":" in arg:
            hosts.append(arg)
        else:
            topics.append(arg)

    procs = []
    duration = float(duration)
    run_producers(procs, count, duration, hosts, topics)
