import threading
import sys
import os
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
        self.my_znode = ""
        self.my_address = my_address
        self.zk_hosts = zk_hosts
        self.zk_client = KazooClient(hosts=makeHostsString(zk_hosts))
        self.brokers = [] 

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
        self.join_cluster()

    def join_cluster(self):

        # The callback that runs when the watch is triggered by a change to the registry 
        # TODO self.brokers list needs to be updated safely 
        def dynamic_watch(watch_information): 
            self.brokers = []
            # Notice that we need to reregister ourself
            nodes = self.zk_client.get_children("/brokerRegistry", watch=dynamic_watch)            
            for b in nodes:
                v, _ = self.zk_client.get("/brokerRegistry/{}".format(b))
                addr = v.decode("utf-8").strip()
                self.brokers.append(addr)
            formatted = ["{} ({})".format(x,y) for x, y in zip(nodes, self.brokers)]
            print("Broker Watch: {}".format(", ".join(formatted)))            
        
        try:
            # TODO does this ordering of operations make sense

            # start the client
            self.zk_client.start()
            self.zk_client.ensure_path("/brokerRegistry")
            
            # create a watch and a new node for this broker
            self.zk_client.get_children("/brokerRegistry", watch=dynamic_watch)
            self.my_znode = self.zk_client.create("/brokerRegistry/broker", ephemeral=True, sequence=True)
            self.zk_client.set(self.my_znode, self.my_address.encode("utf-8"))

        except Exception:
            pass


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python pubsubBroker.py <configuration_path> <zk_config>") 
        exit(1)

    print("Starting PubSub Broker...")

    # Load up the the Broker configuration  
    # TODO: Yml or something would be cool if we feel like it
    my_url = 'localhost:3000' 
    broker_config_path = sys.argv[1]
    zk_config_path = sys.argv[2]

    exists = os.path.isfile(broker_config_path) 
    if exists:
        with open(broker_config_path, "r") as f:
            broker_conf_array = f.readlines()
            my_url = broker_conf_array[0].strip() # Smh

    my_ip_addr = my_url.split(":")[0]
    my_port = int(my_url.split(":")[1])

    # Display the loaded configuration
    print("Address:\t{}".format(my_url))

    # Load up the Supporting Zookeeper Configuration
    zk_client_port = ""
    zk_hosts = [] 
    exists = os.path.isfile(zk_config_path) 
    if exists:
        with open(zk_config_path, "r") as f:
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

    # Create the Broker and Spin up its RPC server
    rpc_server = threadedXMLRPCServer((my_ip_addr, my_port), requestHandler=RequestHandler)
    broker = PubSubBroker(my_url, zk_hosts)

    # Register all functions in the Broker's Public API
    rpc_server.register_introspection_functions()
    rpc_server.register_function(broker.enqueue, "broker.enqueue")
    rpc_server.register_function(broker.enqueue_replica, "broker.enqueue_replica")
    rpc_server.register_function(broker.last_index, "broker.last_index")
    rpc_server.register_function(broker.consume, "broker.consume")

    print("Started successfully... accepting requests. (Halt program to stop.)")

    # Control - TODO should this happen some other time
    service_thread = threading.Thread(target=broker.serve) 
    service_thread.start()

    # Start Broker Server
    rpc_server.serve_forever()
