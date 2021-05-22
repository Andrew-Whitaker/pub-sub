# zk_helpers.py>

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