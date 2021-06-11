import sys
import time
from requests import get
from multiprocessing import Process
from publisher import run_publisher
from stats import *

def run_producers(procs, count: int, duration, hosts, topics):

    # Get unique IDs for the producers
    public_ip = get('https://api.ipify.org').text
    addr_part = public_ip.split('.')[1]

    for i in range(0, count):
        id = addr_part + str(i)
        pubProcess = Process(target=run_publisher, args=(id, topics, hosts, duration))
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
