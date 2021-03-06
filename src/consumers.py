import sys

from pub_sub_scale_test import run_consumers, valid_duration_str
from zk_helpers import get_zookeeper_hosts

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print("Usage: python src/scale/consumers.py <no. cons> <duration (s)> <zk hosts...>") 
        exit(1)

    count = sys.argv[1]
    duration = sys.argv[2]
    hosts = sys.argv[3:]

    if not count.isdigit() or not valid_duration_str(duration):
        print("Usage: python src/scale/consumers.py <no. cons> <duration (s)> <zk hosts...>")
        exit(1)

    procs = []
    duration = float(duration)
    run_consumers(procs, hosts, count, duration)