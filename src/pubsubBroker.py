import threading
import sys
import os
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
from kazoo.client import KazooClient
from kazoo.client import KazooState
import logging
from chordNode import create_chord_ring, check_if_new_leader

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
        self.brokers = [] # array of ChordNodes representing the ChordRing 

        # control data
        self.operational = False

        # Topic data structures
        # keeping it simple...it's just a single integer instead of a map
        # of topic queues
        self.topic_data = 0 
        self.data_lock = threading.Lock()


    # RPC Methods ==========================
    
    def enqueue(self, topic: str, message: str):
        if self.operational:
            self.data_lock.acquire()
            self.topic_data += 1
            logging.debug("Data value: {}".format(str(self.topic_data)))
            self.data_lock.release()
            return True
        else:   
            return False

    def enqueue_replica(self, topic: str, message: str, index: int):
        pass

    def last_index(self, topic: str):
        pass

    def consume(self, topic: str, index: int):
        pass

    # Control Methods ========================

    def state_change_handler(self, conn_state):
        if conn_state == KazooState.LOST:
            logging.warning("Kazoo Client detected a Lost state")
            self.operational = False
        elif conn_state == KazooState.SUSPENDED:
            logging.warning("Kazoo Client detected a Suspended state")
            self.operational = False
        elif conn_state == KazooState.CONNECTED: # KazooState.CONNECTED
            logging.warning("Kazoo Client detected a Connected state")
            self.operational = True
        else:
            logging.warning("Kazoo Client detected an UNKNOWN state")

    def serve(self):
        self.join_cluster()

    def join_cluster(self):

        # The callback that runs when the watch is triggered by a change to the registry 
        # TODO self.brokers list needs to be updated safely 
        def dynamic_watch(watch_information): 
            updated_brokers = [] # array of addresses

            # Notice that we need to reregister the watch
            nodes = self.zk_client.get_children("/brokerRegistry", watch=dynamic_watch)            
            for addr in nodes:
                # v, _ = self.zk_client.get("/brokerRegistry/{}".format(b))
                # addr = v.decode("utf-8").strip()
                updated_brokers.append(addr)

            # build updated Chord Ring
            updated_ring = create_chord_ring(updated_brokers)

            # Check for changes that would imply that the broker should DO SOMETHING
            # predecessor_changed = check_if_new_leader(updated_ring, self.brokers, self.my_address)

            self.brokers = updated_ring

            
            formatted = ["{}".format(str(node)) for node in updated_ring]
            print("Broker Watch: {}".format(", ".join(formatted)))
            # print("Responsible for view change: {}".format(str(predecessor_changed)))            
        
        try:
            # TODO does this ordering of operations make sense

            # start the client
            self.zk_client.add_listener(self.state_change_handler)
            self.zk_client.start()
            self.zk_client.ensure_path("/brokerRegistry")
            
            # create a watch and a new node for this broker
            self.zk_client.get_children("/brokerRegistry", watch=dynamic_watch)
            self.my_znode = self.zk_client.create("/brokerRegistry/{}".format(self.my_address), value="true".encode("utf-8"), ephemeral=True)

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

    service_thread.join()
