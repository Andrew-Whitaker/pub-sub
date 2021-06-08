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
        self.temp_block_segment = (-1, -1) # chord segment that this Broker should service (View Change)

        # Topic data structures 
        self.creation_lock = threading.Lock() # lock for when Broker needs to create a topic
        self.topics = {} # dictionary - (topic: str) => Class Topic
        self.pending_buffers = {} # dictionary - (topic: str) => Class Topic 

    # RPC Methods ==========================

    def enqueue(self, topic: str, message: str):
        if not self.operational:
            print("Not Operational")
            return False

        topic_hash = chord_hash(topic)
        my_job = in_segment_range(topic_hash, self.primary_segment[0], self.primary_segment[1])
        blocked = in_segment_range(topic_hash, self.temp_block_segment[0], self.temp_block_segment[1])
        if not my_job or blocked:
            print("Not My Job ({}) or Blocked ({})".format(not my_job, blocked))
            return False
        
        # protect against contention when creating topics 
        if not self.topics.get(topic, None):
            self.creation_lock.acquire()
            if not self.topics.get(topic, None):
                self.topics[topic] = Topic(topic)
            self.creation_lock.release()

        # atomically assigns an index to the message
        message_index = self.topics[topic].publish(message)

        # who are my successors
        repl1, repl1_index = find_chord_successor(self.my_address, self.brokers)
        repl2, repl2_index = find_chord_successor(repl1.key, self.brokers, repl1_index)

        succ_one_exception = False
        succ_two_exception = False
        try:
            if repl1.key != self.my_address:
                r1Client = buildBrokerClient(repl1.key)
                success_one = r1Client.broker.enqueue_replica(topic, message, message_index - 1)
        except Exception as e:
            succ_one_exception = True

        try:
            if repl2.key != self.my_address:
                r2Client = buildBrokerClient(repl2.key)
                success_two = r2Client.broker.enqueue_replica(topic, message, message_index - 1)
        except Exception as e:
            succ_two_exception = True

        if succ_one_exception and succ_two_exception:
            print("A PubSub Assumption Was Violated: Terminating this Broker")
            exit(1)

        return True

    def enqueue_replica(self, topic: str, message: str, index: int):
        if not self.operational: 
            return False
        
        # protect against contention when creating topics 
        if not self.topics.get(topic, None):
            self.creation_lock.acquire()
            if not self.topics.get(topic, None):
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
        if not self.topics.get(topic, None):
            return 0
        return self.topics[topic].next_index() 

    def consume(self, topic: str, index: int):
        if not self.operational or not self.topics.get(topic, None):
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
        logging.warning("Broker {} is blocking requests for [{},{}]".format(
            self.my_address, str(start), str(end)))

        # 1) Change Temp Blocked Segment Range
        self.temp_block_segment = (start, end)

        # 2) Fill in Topic Vector Map with Next Available Index
        topic_vector = {} # Example: {"sport": 13, "politics": 98} # (topic_name, index)
        for name, topic in self.topics.items():
            # only add topic queues from this segment
            if in_segment_range(chord_hash(name), start, end):
                topic_vector[name] = topic.next_index()

        return self.curr_view, topic_vector

    def consume_bulk_data(self, start, end):
        """This func is called via RPC by another broker. Usually called when
        a broker needs to get "up to speed" with some new topics it is
        responsible for.

        Returns all topic data that fall in the range of start, end
        """

        # the data we want to return
        data = {}

        # find which belong to you
        for name, topic in self.topics.items():
            t_hash = chord_hash(name)
            # if the hash is in your primary range
            if in_segment_range(t_hash, start, end):
                # add the topic data to the data to be returned
                data[name] = topic.consume(0)

        return data

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
                self.perform_replica_sync(segment, pred)

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

            # do something with the Topic Vector
            self.prepare_as_primary(topic_vector)

        else:
            prev_view = 0
        
        self.curr_view = prev_view
        logging.warning("Broker {} is starting view {}. Responsible for [{},{}]".format(
            self.my_address, str(prev_view + 1), str(start), str(end)))

        # 4) Jump into the mix by registering in ZooKeeper
        self.join_cluster()

        
    def join_cluster(self):
        try:
            # enable RPC requests to come through
            self.operational = True

            # create a watch and a new node for this broker
            self.zk_client.ensure_path(BROKER_REG_PATH)
            self.zk_client.get_children(BROKER_REG_PATH, watch=self.registry_callback)
            my_path = BROKER_REG_PATH + "/{}".format(self.my_address)
            self.my_znode = self.zk_client.create(my_path, value="true".encode("utf-8"), ephemeral=True)

        except Exception as e:
            logging.warning("Join Cluster error: {}".format(e))
            self.operational = False
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
        print(np_start, " -- ", np_end)
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

    # Given a topic map of topics that need updating, reach out to
    def prepare_as_primary(self, topics):
        # Find Current Primary of the segment that you'll use to get up to date
        curr_primary, _ = find_chord_successor(self.my_address, self.brokers)
        rpc_primary = buildBrokerClient(curr_primary.key)

        # Loop Through topics and update local topic queues
        for name, global_next_index in topics.items():
            self.update_topic(name, global_next_index, rpc_primary)
            print("Updating topic: {} until {}".format(name, str(global_next_index)))


    def update_topic(self, topic_name: str, goal_index: int, rpc_broker):
        # Create Topic if it doesn't already exist
        if not self.topics.get(topic_name, None):
            self.creation_lock.acquire()
            if not self.topics.get(topic_name, None):
                self.topics[topic_name] = Topic(topic_name)
            self.creation_lock.release()
        # Get Next Index that this broker needs locally
        my_next_index = self.topics[topic_name].next_index()
        # Consume data from other broker until you've reached global next index
        while goal_index > my_next_index:
            partial_log = rpc_broker.broker.consume(topic_name, my_next_index)
            for message in partial_log:
                self.topics[topic_name].publish(message)
            my_next_index = self.topics[topic_name].next_index()


    def perform_view_change_sync(self, segment, successor):
        if self.my_address != successor.key:
            logging.warning("Broker {} is performing view change with {} for segment[{}, {}]".format(
                    self.my_address, successor.key, str(segment[0]), str(segment[1])))
        return

    def perform_replica_sync(self, segment, predecessor):
        if segment[0] != -1 and segment[1] != -1:
            logging.warning("Broker {} is updating replicas with {} for segment[{}, {}]".format(
                    self.my_address, predecessor.key, str(segment[0]), str(segment[1])))

            # create rpc client
            client = buildBrokerClient(predecessor.key)

            # get the data
            data = client.broker.consume_bulk_data(segment[0], segment[1])

            # set local state - create Topic objects and add them to our dict
            self.creation_lock.acquire()
            for topic in data:
                t_obj = Topic(topic)
                t_obj.messages = data[topic]
                self.topics[topic] = t_obj
                #print("======\n{}\n{}\n======".format(type(self.topics[topic]), self.topics[topic]))
            self.creation_lock.release()

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

    def primary_topics(self):
        """Returns the list of topics this node is a primary for
        """
        pt = []
        for t_name in self.topics:
            t_hash = chord_hash(t_name)
            if in_segment_range(t_hash, self.primary_segment[0], self.primary_segment[1]):
                pt.append(t_name)

        return pt

    def replica_topics(self):
        """Returns the list of topics this node is a replica for
        """
        pt = []
        for t_name in self.topics:
            t_hash = chord_hash(t_name)
            if in_segment_range(t_hash, self.replica_segment[0], self.replica_segment[1]):
                pt.append(t_name)
        return pt

        return data
    def cli(self):
        while True:
            try:
                ipt = input("\n> ")
                tokens = ipt.split(" ")
                cmd = tokens[0]
                arg = None
                if len(tokens) > 1:
                    arg = tokens[1]

                if cmd == "pseg":
                    print(self.primary_segment)
                elif cmd == "rseg":
                    print(self.replica_segment)
                elif cmd == "ptop":
                    print(self.primary_topics())
                elif cmd == "rtop":
                    print(self.replica_topics())
                elif cmd == "topics":
                    val = list(self.topics.keys())
                    val.sort()
                    print(val)
                elif cmd == "view":
                    print(self.curr_view)
                elif cmd == "brokers":
                    print(self.brokers) # this doesn't pretty print?
                elif cmd == "topic": # a specific topic
                    if arg:
                        print(self.topics[arg].messages[-10:])
                    else: # all the topic values
                        for topic in self.topics:
                            print("{} : {}".format(topic, self.topics[topic].messages[-10:]))

                elif cmd != "": #help
                    hint = "Available commands\n" + \
                            "'pseg' -> primary segment\n" + \
                            "'rseg' -> replica segment\n" + \
                            "'ptop' -> primary topic\n" + \
                            "'rtop' -> replica topic\n" + \
                            "'topics' -> list of topics\n" + \
                            "'topic x' -> last 10 messages in topic 'x'\n" + \
                            "'view' -> current view number\n" + \
                            "'brokers' -> list of all brokers"
                    print(hint)
            except Exception as e:
                print("Error:", e)
            # TODO cli
            # - brokers in order of chord ring. Format to help viz-
            #       node-1 (x,     y)
            #       node-2 (y+1,   z)
            #       node-3 (z+1, x-1)
            # Also if the terminal is getting too messy,
            # redirect log output of all processes to a file and do "tail -f"


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

    # CLI for debugging - will be messy due to log outputs
    cli_thread = threading.Thread(target=broker.cli)
    cli_thread.start()

    # Start Broker RPC Server
    rpc_server.serve_forever()

    service_thread.join()
    cli_thread.join()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python src/pubsubBroker.py <broker_addr> <zk_config>") 
        exit(1)

    print("Starting PubSub Broker...")
    broker_address = sys.argv[1]
    zk_config_path = sys.argv[2]

    # Display the loaded configuration
    start_broker(zk_config_path, broker_address)
