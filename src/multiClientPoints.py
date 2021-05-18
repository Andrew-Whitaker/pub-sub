from kazoo.client import KazooClient
import threading
import time

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




if __name__ == "__main__":
   
    t0 = threading.Thread(target = runClient, args = (0,))
    t0.start()
    t1 = threading.Thread(target = runClient, args = (1,))
    t1.start()

    t0.join()
    t1.join()

