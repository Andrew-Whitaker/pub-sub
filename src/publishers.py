import sys

from pub_sub_scale_test import run_producers, valid_duration_str
from zk_helpers import get_zookeeper_hosts

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage: python src/scale/publishers.py <zk_config> <no. pubs> <duration (s)>") 
        exit(1)

    zk_config_path = sys.argv[1]
    count = sys.argv[2]
    duration = sys.argv[3]

    if not count.isdigit() or not valid_duration_str(duration):
        print("Usage: python src/scale/publishers.py <zk_config> <no. pubs> <duration (s)>") 
        exit(1)

    hosts = get_zookeeper_hosts(zk_config_path)
    procs = []
    duration = float(duration)
    run_producers(procs, hosts, count, duration)
