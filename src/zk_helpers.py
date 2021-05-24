# zk_helpers.py>
import sys
import os

def makeHostsString(hosts):
    # In case hosts is an empty array
    if len(hosts) < 1:
        return ""

    # start with first host
    hostsString = hosts[0]

    # Append all hosts after that with comma separation
    for host in hosts[1:]:
        hostsString += ("," + host)

    return hostsString

def get_zookeeper_hosts(config_path):
    # Load up the Supporting Zookeeper Configuration
    zk_client_port = ""
    zk_hosts = [] 
    exists = os.path.isfile(config_path) 
    if exists:
        with open(config_path, "r") as f:
            zk_conf_array = f.readlines()
            for l in zk_conf_array:
                if l.startswith("server."): 
                    fst, snd = l.split("=")
                    cleaned = snd.split(":", 1)[0].strip() # TODO Smh
                    zk_hosts.append(cleaned)
                if l.startswith("clientPort"):
                    fst, snd = l.split("=")
                    zk_client_port = snd.strip()

    zk_hosts = ["{}:{}".format(z, zk_client_port) for z in zk_hosts]
    print("Zookeepers:\t{}".format(", ".join(zk_hosts)))
    return zk_hosts

