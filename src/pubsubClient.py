from kazoo.client import KazooClient
import xmlrpc.client
import sys
from zk_helpers import *
from chord_node import *
import time

class PubSubClient:

    def __init__(self, zk_hosts):
        self.zk_hosts = zk_hosts
        self.brokers = [] # array of ChordNode objects

        # maintain ZooKeeper connection
        self.zk_client = KazooClient(makeHostsString(zk_hosts))
        self.zk_client.start()

        # set up local broker registry and watch for changes
        # The callback that runs when the watch is triggered by a change to the registry 
        # TODO self.brokers list needs to be updated safely 
        def dynamic_watch(watch_information): 
            updated_brokers = [] # array of addresses

            # Notice that we need to reregister the watch
            nodes = self.zk_client.get_children("/brokerRegistry", watch=dynamic_watch)            
            for addr in nodes:
                updated_brokers.append(addr)

            # build updated Chord Ring
            self.brokers = create_chord_ring(updated_brokers)

            formatted = ["{}".format(str(node)) for node in self.brokers]
            print("Broker Watch: {}".format(", ".join(formatted)))
            # print("Responsible for view change: {}".format(str(predecessor_changed)))

        # SUCH A BAD HACK. I NEED THE FIRST BROKER RING AND TO SET THE WATCH
        # TODO: Remove duplicated code and get rid of this inline function
        updated_brokers = [] # array of addresses
        # Notice that we need to reregister the watch
        nodes = self.zk_client.get_children("/brokerRegistry", watch=dynamic_watch)            
        for addr in nodes:
            updated_brokers.append(addr)
        # build updated Chord Ring
        self.brokers = create_chord_ring(updated_brokers)

    def create_topic(self):
        # can this just be nothing?
        pass

    def delete_topic(self):
        pass

    def topic_exists(self):
        pass

    def publish(self, topic: str, message: str):
        # Find the right Broker
        broker = find_chord_successor(topic, self.brokers)
        # set up RPC-client
        broker_rpc = buildBrokerClient(broker[0].key)
        # Send message
        return broker_rpc.broker.enqueue(topic, message)

    def consume(self, topic: str, index: int):
        broker = find_chord_successor(topic, self.brokers)
        broker_rpc = buildBrokerClient(broker[0].key)
        return broker_rpc.broker.consume(topic, index)

    #==============================
    # Private Methods

def buildBrokerClient(host: str):
    ''' Build a client for the Broker RPC server
    Paramters:
    host (str): Address and Port of the Broker. Takes the form of "ip_address:port_number". 
        Ex: "127.0.0.1:2181"

    Returns:
    ServerProxy: RPC client for the Broker at the host address

    '''
    # Set up url properly
    url = "http://" + host
    rpc_client = xmlrpc.client.ServerProxy(url)
    return rpc_client


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: python pubsubClient.py <zk_config>") 
        exit(1)

    print("Starting PubSub Client...")
    zk_config_path = sys.argv[1]
    zk_hosts = get_zookeeper_hosts(zk_config_path)

    psclient = PubSubClient(zk_hosts)

    # ====================
    # Simple use of PS client to see that the connection to Brokers works

    for i in range(100):
        psclient.publish(str(i) + "topic", "message")
        time.sleep(1)







