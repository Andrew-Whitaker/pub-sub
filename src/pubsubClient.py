from kazoo.client import KazooClient
import xmlrpc.client
import sys, logging
from zk_helpers import *
from chord_node import *
import time

logging.basicConfig(level=logging.WARNING)

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

        # TODO: Remove duplicated code and get rid of this inline function
        updated_brokers = [] # array of addresses
        # Notice that we need to reregister the watch
        nodes = self.zk_client.get_children("/brokerRegistry", watch=dynamic_watch)            
        for addr in nodes:
            updated_brokers.append(addr)
        # build updated Chord Ring
        self.brokers = create_chord_ring(updated_brokers)

    def publish(self, topic: str, message: str):
        published = False
        while not published:
            try: 
                # Find the right Broker
                broker, _ = find_chord_successor(topic, self.brokers)
                print("Client trying to publish to {}".format(broker.key))
                # set up RPC-client
                broker_rpc = buildBrokerClient(broker.key)
                # Send message
                published = broker_rpc.broker.enqueue(topic, message)
                if published: return True
            except Exception as e:
                logging.warning(e)
            time.sleep(1)


    def consume(self, topic: str, index: int):
        while True:
            try: 
                broker, _ = find_chord_successor(topic, self.brokers)
                print("Client trying to consume from {}".format(broker.key))
                broker_rpc = buildBrokerClient(broker.key)
                return broker_rpc.broker.consume(topic, index)
            except Exception as e:
                logging.warning(e)
            time.sleep(1)

    def primary(self, topic):
        broker = find_chord_successor(topic, self.brokers)
        return broker

    def fetch_brokers(self):
        return self.brokers

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
        psclient.publish("topic-"+f'{i:03d}', "message")
        time.sleep(1)







