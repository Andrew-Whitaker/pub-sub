import threading
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
from kazoo.client import KazooClient
import logging

from zk_helpers import *


logging.basicConfig()

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class threadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass


class PubSubBroker:

    def __init__(self, my_address, zk_hosts):
        self.my_address = my_address
        self.zk_hosts = zk_hosts
        self.zk_client = KazooClient(hosts=makeHostsString(zk_hosts))
        self.brokers = [] # empty list of ChordNodes for Brokers

    # RPC Methods ==========================
    
    def enqueue(self, topic: str, message: str):
        pass

    def enqueue_replica(self, topic: str, message: str, index: int):
        pass

    def last_index(self, topic: str):
        pass

    def consume(self, topic: str, index: int):
        pass

    # Control Methods ========================

    def serve(self):
        
        # Probably needs to be some sort of control loop
        # so that Broker can react to changes 
        self.join_cluster()

        pass

    def join_cluster(self):
    
        try:
            self.zk_client.start()
            self.zk_client.ensure_path("/brokerRegistry")

            self.zk_client.set()

        except Exception:
            pass



if __name__ == "__main__":

    # Get My Url
    my_url = 'localhost:3000' # read from command line or file
    my_ip_addr = my_url.split(":")[0]
    my_port = int(my_url.split(":")[1])

    # Get ZooKeeper Config file and find ZK host addresses
    zk_hosts = [] 
    
    # Create the Broker and Spin up its RPC server
    rpc_server = threadedXMLRPCServer((my_ip_addr, my_port), requestHandler=RequestHandler)
    broker = PubSubBroker(my_url, zk_hosts)

    # Register all functions in the Broker's Public API
    rpc_server.register_introspection_functions()
    rpc_server.register_function(broker.enqueue, "broker.enqueue")
    rpc_server.register_function(broker.enqueue_replica, "broker.enqueue_replica")
    rpc_server.register_function(broker.last_index, "broker.last_index")
    rpc_server.register_function(broker.consume, "broker.consume")

    print("Started successfully.")
    print("Accepting requests. (Halt program to stop.)")

    # Control 
    threading.Thread(target=broker.serve) 

    # Start Broker Server
    rpc_server.serve_forever()
