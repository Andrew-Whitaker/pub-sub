from kazoo.client import KazooClient
import threading
import time

from kazoo.exceptions import NoNodeError
from kazoo.handlers.threading import KazooTimeoutError

def encodeMessage(value: int):
    return str(value).encode('utf-8')


def runClient(id: int):
    print("Starting Cl" + str(id))
    zk = KazooClient(hosts='127.0.0.1:2181')
    zk.start()

    root = zk.get_children("/")
    print("Cl" + str(id) + ")   root: " + str(root))

    zk.ensure_path("/testClients")
    myPath = "/testClients/" + str(id)
    theirPath = "/testClients/"
    if id:
        theirPath += str(0)
    else:
        theirPath += str(1)
    
    if zk.exists(myPath) == False:
        zk.create(myPath, encodeMessage(id))

    for i in range(20):
        zk.set(myPath, encodeMessage(i))
        if zk.exists(theirPath):
            data, stat = zk.get(theirPath)
            print("Cl" + str(id) +" read " + data.decode('utf-8') + " from: " + theirPath)
        time.sleep(1)

    zk.stop()
    return


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



if __name__ == "__main__":
   
    # Create array of host addresses
    hosts = ['localhost:2181']

    # multiple hosts
    #hosts = ['127.0.0.1:2182', '127.0.0.1:2183', '127.0.0.1:2184']
    
    # Write a piece of data to ZooKeeper from a client
    dataPath = "/verify"
    secretKey = 13 # cuz it's the best number in the world
    zk = KazooClient(hosts=makeHostsString(hosts))
    try:
        zk.start()
        if zk.exists(dataPath):
            zk.set(dataPath, encodeMessage(secretKey))
        else:
            zk.create(dataPath, encodeMessage(secretKey))
        
    except KazooTimeoutError:
        print("Wasn't able to perform any connections.")
        exit()
    except Exception:
        print("??????")
        exit()

    # Try to read the secret key off of all the ZooKeeper servers
    # one at a time to verify they are alive and connected together
    for host in hosts:
        zk = KazooClient(hosts=makeHostsString([host]))
        try:
            zk.start()
            data, stat = zk.get(dataPath)
            if data.decode('utf-8') == str(secretKey):
                print("SUCCESS: Verified " + host + " connected and synced.")
            else:
                print("FAILURE: can connect to " + host + " but not in sync.")
        
        except KazooTimeoutError:
            print("FAILURE: Cannot connect to " + host)

        except NoNodeError:
            print("FAILURE: can connect to " + host + " but not in sync.")
        

