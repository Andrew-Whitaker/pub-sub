import threading, queue
import sys, os, time
import logging
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.exceptions import KazooException, OperationTimeoutError
from kazoo.protocol.paths import join

from chord_node import *
from topic import Topic, consuming_enqueue 
from event import *
from zk_helpers import *
from pubsubClient import buildBrokerClient

BROKER_REG_PATH = "/brokerRegistry"

logging.basicConfig(level=logging.WARNING)

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
        self.zk_client.add_listener(self.state_change_handler)
        self.brokers = [] # array of ChordNodes representing the ChordRing 

        # Let Broker Control Functionality by responding to events
        self.event_queue = queue.Queue()
        self.operational = False # RPC method should/not accept requests
        self.curr_view = 0 # view number

        # Topic Responsibilities
        self.primary_segment = (-1,-1) 
        self.replica_segment = (-1,-1)

        # Topic data structures 
        self.creation_lock = threading.Lock() # lock for when Broker needs to create a topic
        self.topics = {} # dictionary - (topic: str) => Class Topic
        self.pending_buffers = {} # dictionary - (topic: str) => Class Topic 

    # RPC Methods ==========================

    def enqueue(self, topic: str, message: str):
        if not self.operational:
            return False
        
        # protect against contention when creating topics 
        self.creation_lock.acquire()
        if topic not in self.topics:
            self.topics[topic] = Topic(topic)
        self.creation_lock.release()

        # who are my successors
        repl1, repl1_index = find_chord_successor(self.my_address, self.brokers)
        repl2, repl2_index = find_chord_successor(repl1.key, self.brokers, repl1_index)

        # create a client for each of the replicas
        r1Client = buildBrokerClient(repl1.key)
        r2Client = buildBrokerClient(repl2.key)

        # atomically assigns an index to the message 
        message_index = self.topics[topic].publish(message)
        
        # By construction, we assume at least one of these (TODO ideally concurrent) calls will succeed
        success_one = r1Client.broker.enqueue_replica(topic, message, message_index)
        success_two = r2Client.broker.enqueue_replica(topic, message, message_index)

        return True

    def enqueue_replica(self, topic: str, message: str, index: int):
        if not self.operational: 
            return False
        
        # protect against contention when creating topics 
        self.creation_lock.acquire()
        if topic not in self.topics:
            self.topics[topic] = Topic(topic)
        self.creation_lock.release() 

        broker = find_chord_successor(topic, self.brokers)
        broker_rpc_client = buildBrokerClient(broker[0].key)

        # attempts to move the commit point as aggressiively as possible
        consuming_enqueue(self.topics[topic], broker_rpc_client, message, index)
        return True

    def last_index(self, topic: str):
        if not self.operational:
            return -1 
        if topic not in self.topics:
            return 0
        return self.topics[topic].next_index() 

    def consume(self, topic: str, index: int):
        if not self.operational or topic not in self.topics:
            return []
        return self.topics[topic].consume(index)

    def get_queue(self, topic):
        return self.topics[topic].messages

    def get_topics(self):
        return list(self.topics.keys())


    def request_view_change(self, start: int, end: int):
        """This broker is being requested by another broker to perform a view change.
        Other Broker (new primary) wants to take responsibility for the segment of 
        the chord ring [start, end]. 
        
        This broker needs to lock all of the topic channels it has between start and end,
        and cease taking user requests for these topics and provide the new broker with the
        index of the queue that it can begin pushing messages to.
        
        """
        topic_vector = {} # Example: {"sport": 13, "politics": 98} # (topic_name, index)
        # Go through topic queus and perform proper operations

        logging.warning("Broker {} is blocking requests for [{},{}]".format(
            self.my_address, str(start), str(end)))

        return self.curr_view, topic_vector

    def consume_bulk_data(self, start, end):
        """This func is called via RPC by another broker. Usually called when
        a broker needs to get "up to speed" with some new topics it is
        responsible for.

        Returns all topic data that fall in the range of start, end
        """

        # the data we want to return
        data {}

        # get the topics
        topics = list(self.topics.keys())

        # find which belong to you
        for topic in topics:
            t_hash = chord_hash(topic)
            # if the hash is in your primary range
            if in_segment_range(t_hash, start, end):
                # add the topic data to the data to be returned
                data[topic] = self.topics[topic].consume(0)

        return data


    def get_replica_data(self, start, end):
        # create rpc client
        pred = find_chord_predecessor(self.my_address, self.brokers)
        client = buildBrokerClient(pred.key)

        # get the data
        data = client.broker.consume_bulk_data(start, end)

        # set local state
        self.creation_lock.acquire()
        for topic in data:
            self.topics[topic] = data[topic]
        self.creation_lock.release()


    # Control Methods ========================

    def serve(self):
        # start process of joining the system
        self.event_queue.put(ControlEvent(EventType.RESTART_BROKER))

        while True: # infinite Broker serving loop
            # Wait for an event off the communication channel
            # and respond to it
            event = self.event_queue.get() # blocking call

            if event.name == EventType.PAUSE_OPER:
                self.operational = False
            elif event.name == EventType.RESUME_OPER:
                # Don't quite know what will need to be done in this situation
                # 1) Get an updated chord ring because no guarantees that it 
                #    is still the same since we were last connected. 
                # 2) This may also imply some catch up on data!
                # 2) Make RPC server operational 
                pass
            elif event.name == EventType.RESTART_BROKER:
                # retry Making connection with ZooKeeper and joining the cluster
                self.restart_broker()
            elif event.name == EventType.RING_UPDATE:
                # Take care of new updated chord ring
                ring = event.data[CHORD_RING]
                self.manage_ring_update(ring)
                # reset watch on Broker Registry in ZooKeeper
                self.zk_client.get_children(BROKER_REG_PATH, watch=self.registry_callback)

            elif event.name == EventType.UPDATE_TOPICS:
                segment = event.data[SEGMENT] # segment of chord ring in question
                pred, _ = find_chord_predecessor(self.my_address, self.brokers)
                logging.warning("Broker {} is updating replicas with {} for segment[{}, {}]".format(
                    self.my_address, pred.key, str(segment[0]), str(segment[1])))
                self.get_replica_data(segment[0], segment[1])

            elif event.name == EventType.VIEW_CHANGE:
                segment = event.data[SEGMENT] # segment of chord ring in question
                succ,_ = find_chord_successor(self.my_address, self.brokers)
                self.perform_view_change_sync(segment, succ)
            else:
                logging.warning("Unknown Event detected: {}".format(event.name))

    def restart_broker(self):
        connected = False
        while not connected:
            try: 
                # start the client
                self.zk_client.start()
                connected = True
            except Exception as e:
                logging.warning("Join Cluster error: {}".format(e))

        try:
            # build chord ring for the first time
            self.zk_client.ensure_path(BROKER_REG_PATH)
            broker_addrs = self.zk_client.get_children(BROKER_REG_PATH) 
            self.brokers = create_chord_ring(broker_addrs)
        except Exception as e:
            logging.warning("Join Cluster error: {}".format(e))
            self.event_queue.put(ControlEvent(EventType.RESTART_BROKER))
            return

        # TODO Request a View Change from the previous Primary
        # 1) determine topic range this broker will inhabit
        start, end = find_prime_chord_segment(self.my_address, self.brokers)

        # 2) determine who the previous primary is
        curr_primary, _ = find_chord_successor(self.my_address, self.brokers)

        # 3) request view change for that keyspace
        if curr_primary != None:
            # set up RPC-client
            broker_rpc = buildBrokerClient(curr_primary.key)
            prev_view, topic_vector = broker_rpc.broker.request_view_change(start, end)
        else:
            prev_view = 0
        
        self.curr_view = prev_view
        logging.warning("Broker {} is starting view {}. Responsible for [{},{}]".format(
            self.my_address, str(prev_view + 1), str(start), str(end)))

        # 4) Jump into the mix by registering in ZooKeeper
        self.join_cluster()

        
    def join_cluster(self):
        try:           
            # create a watch and a new node for this broker
            self.zk_client.ensure_path(BROKER_REG_PATH)
            self.zk_client.get_children(BROKER_REG_PATH, watch=self.registry_callback)
            my_path = BROKER_REG_PATH + "/{}".format(self.my_address)
            self.my_znode = self.zk_client.create(my_path, value="true".encode("utf-8"), ephemeral=True)

            # enable RPC requests to come through
            self.operational = True

        except Exception as e:
            logging.warning("Join Cluster error: {}".format(e))
            time.sleep(1)
            self.event_queue.put(ControlEvent(EventType.RESTART_BROKER))

    def manage_ring_update(self, updated_ring):
        # Print to logs
        self.curr_view += 1
        formatted = ["{}".format(str(node)) for node in updated_ring]
        logging.warning("Broker view {} -- Watch: {}".format(
            str(self.curr_view),", ".join(formatted)))

        # Detect if this broker should respond to changes in its Primary segment
        # np_start => new primary start    cp_start => current primary start
        np_start, np_end = find_prime_chord_segment(self.my_address, updated_ring)
        (cp_start, cp_end) = self.primary_segment

        curr_range = segment_range(cp_start, cp_end)
        new_range = segment_range(np_start, np_end)
        if new_range > curr_range: # gained responsibility
            if (cp_start == -1): delta_end = np_end
            elif (cp_start == 0): delta_end = MAX_HASH - 1
            else: delta_end = cp_start - 1
            view_change = ControlEvent(EventType.VIEW_CHANGE, {SEGMENT: (np_start, delta_end)})
            self.event_queue.put(view_change)
        # No need to do anything if range is smaller or the same

        # Detect if this Broker should respond to changes in its Replica Segment
        nr_start, nr_end = find_repl_chord_segment(self.my_address, updated_ring)
        logging.warning("Repl Chord Ring segment[{}, {}]".format(
                    str(nr_start), str(nr_end)))
        (cr_start, cr_end) = self.replica_segment

        curr_range = segment_range(cr_start, cp_end) # use the whole range Replica + Primary
        new_range = segment_range(nr_start, np_end)  # Same here
        if new_range > curr_range: # gained responsibility
            if (cr_start == -1): delta_end = nr_end
            elif (cr_start == 0): delta_end = MAX_HASH - 1
            else: delta_end = cr_start - 1
            view_change = ControlEvent(EventType.UPDATE_TOPICS, {SEGMENT: (nr_start, delta_end)})
            self.event_queue.put(view_change)
        # No need to do anything if range is smaller or the same

        # Replace local cached copy with new ring
        self.brokers = updated_ring
        self.primary_segment = (np_start, np_end)
        self.replica_segment = (nr_start, nr_end)
        return

    def perform_view_change_sync(self, segment, predecessor):
        if self.my_address != predecessor.key:
            logging.warning("Broker {} is performing view change with {} for segment[{}, {}]".format(
                    self.my_address, predecessor.key, str(segment[0]), str(segment[1])))
        return

    def registry_callback(self, watch_event):
        # build updated chord ring
        broker_addrs = self.zk_client.get_children(BROKER_REG_PATH) 
        updated_ring = create_chord_ring(broker_addrs)

        # send event back to Broker controller
        data = {CHORD_RING: updated_ring}
        event = ControlEvent(EventType.RING_UPDATE, data)
        self.event_queue.put(event)
        return

    def state_change_handler(self, conn_state):
        if conn_state == KazooState.LOST:
            logging.warning("Kazoo Client detected a Lost state")
            self.event_queue.put(ControlEvent(EventType.RESTART_BROKER))
        elif conn_state == KazooState.SUSPENDED:
            logging.warning("Kazoo Client detected a Suspended state")
            self.event_queue.put(ControlEvent(EventType.PAUSE_OPER))
        elif conn_state == KazooState.CONNECTED: # KazooState.CONNECTED
            logging.warning("Kazoo Client detected a Connected state")
            self.event_queue.put(ControlEvent(EventType.RESUME_OPER))
        else:
            logging.warning("Kazoo Client detected an UNKNOWN state")

def start_broker(zk_config_path, url):
    ip_addr = url.split(":")[0]
    port = int(url.split(":")[1])

    # Load up the Supporting Zookeeper Configuration
    zk_hosts = get_zookeeper_hosts(zk_config_path)

    # Create the Broker and Spin up its RPC server
    rpc_server = threadedXMLRPCServer((ip_addr, port), requestHandler=RequestHandler)
    broker = PubSubBroker(url, zk_hosts)

    # Register all functions in the Broker's Public API
    rpc_server.register_introspection_functions()
    rpc_server.register_function(broker.enqueue, "broker.enqueue")
    rpc_server.register_function(broker.enqueue_replica, "broker.enqueue_replica")
    rpc_server.register_function(broker.last_index, "broker.last_index")
    rpc_server.register_function(broker.consume, "broker.consume")
    rpc_server.register_function(broker.consume_bulk_data, "broker.consume_bulk_data")
    rpc_server.register_function(broker.request_view_change, "broker.request_view_change")
    
    # Hidden RPCs to support REPL debugging
    rpc_server.register_function(broker.get_queue, "broker.get_queue")
    rpc_server.register_function(broker.get_topics, "broker.get_topics")

    # Control Broker management
    service_thread = threading.Thread(target=broker.serve) 
    service_thread.start()

    # Start Broker RPC Server
    rpc_server.serve_forever()

    service_thread.join()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python src/pubsubBroker.py <configuration_path> <zk_config>") 
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

    # Display the loaded configuration
    start_broker(zk_config_path, my_url)
