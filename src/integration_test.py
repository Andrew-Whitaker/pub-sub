import sys
import os
import time
import unittest

from zk_helpers import get_zookeeper_hosts
from pubsubBroker import start_broker
from pubsubClient import PubSubClient
from multiprocessing import Process
from chord_node import *

# ======= TEST CASES =======

ZK_CONFIG_PATH = '../apache-zookeeper-3.5.9-bin/conf/zoo.cfg'


# class PrimaryTests(unittest.TestCase):

#     def test_primary_catches_up_from_join(self):
#         broker_addrs = ['localhost:3001', 'localhost:3002', 'localhost:3003', 'localhost:3004']
#         broker_nodes = create_chord_ring(broker_addrs)
#         broker_procs = list(range(len(broker_addrs)))
#         topic = "test_topic"
#         # 1) establish which broker will start and which will take over
#         true_broker, true_index = find_chord_successor(topic, broker_nodes)
#         first_broker, first_index = find_chord_successor(true_broker.key, broker_nodes)

#         # 2) Start first broker and fill it with data
#         broker_procs[first_index] = Process(target=start_broker, args=(ZK_CONFIG_PATH, first_broker.key))
#         broker_procs[first_index].start()
#         time.sleep(0.5)

#         # 3) Start Client and fill it with data
#         psclient = PubSubClient(get_zookeeper_hosts(ZK_CONFIG_PATH))
#         data = ["zero", "one", "two", "three", "four", "five"]
#         for message in data:
#             psclient.publish(topic, message)

#         # 4) Start True broker (who will take over as Primary) 
#         # and see if the data transferred over properly
#         broker_procs[true_index] = Process(target=start_broker, args=(ZK_CONFIG_PATH, true_broker.key))
#         broker_procs[true_index].start()
#         time.sleep(0.5)
#         results = psclient.consume(topic, 0)

#         self.assertEqual(data, results)

#         broker_procs[first_index].terminate()
#         broker_procs[true_index].terminate()
#         time.sleep(10)

    # def test_primary_switch_back(self):
    #     broker_addrs = ['localhost:3005', 'localhost:3006', 'localhost:3007', 'localhost:3008']
    #     broker_nodes = create_chord_ring(broker_addrs)
    #     broker_procs = list(range(len(broker_addrs)))
    #     topic = "test_topic"
    #     # 1) establish which broker will start and which will take over
    #     true_broker, true_index = find_chord_successor(topic, broker_nodes)
    #     first_broker, first_index = find_chord_successor(true_broker.key, broker_nodes)

    #     # 2) Start first broker and fill it with data
    #     broker_procs[first_index] = Process(target=start_broker, args=(ZK_CONFIG_PATH, first_broker.key))
    #     broker_procs[first_index].start()
    #     time.sleep(0.5)

    #     # 3) Start Client and fill topic with data
    #     psclient = PubSubClient(get_zookeeper_hosts(ZK_CONFIG_PATH))
    #     data = ["zero", "one", "two", "three", "four", "five"]
    #     for message in data:
    #         psclient.publish(topic, message)

    #     # 4) Start True broker (who will take over as Primary) 
    #     # and see if the data transferred over properly
    #     broker_procs[true_index] = Process(target=start_broker, args=(ZK_CONFIG_PATH, true_broker.key))
    #     broker_procs[true_index].start()
    #     time.sleep(1)

    #     # 5) Kill True Primary to see if First broker will retake reins correctly
    #     broker_procs[true_index].terminate()
    #     time.sleep(10)

    #     results = psclient.consume(topic, 0)
    #     self.assertEqual(data, results)

    #     # 6) Clean up
    #     broker_procs[first_index].terminate()
        

class ReplicationTests(unittest.TestCase):
    def test_replica_catches_up_from_join(self):
        broker_addrs = ['localhost:4001', 'localhost:4002', 'localhost:4003', 'localhost:4004']
        broker_nodes = create_chord_ring(broker_addrs)
        broker_procs = list(range(len(broker_addrs)))
        topic = "test_topic"
        # 1) establish which broker will start and which will become the replica
        true_broker, true_index = find_chord_successor(topic, broker_nodes)
        repl_broker, repl_index = find_chord_successor(true_broker.key, broker_nodes)

        # 2) Start true broker and fill it with data
        broker_procs[true_index] = Process(target=start_broker, args=(ZK_CONFIG_PATH, true_broker.key))
        broker_procs[true_index].start()
        time.sleep(0.5)

        # 3) Start Client and fill it with data
        psclient = PubSubClient(get_zookeeper_hosts(ZK_CONFIG_PATH))
        data = ["zero", "one", "two", "three", "four", "five"]
        for message in data:
            psclient.publish(topic, message)

        # 4) Start Replica broker and see if the data transferred over properly
        broker_procs[repl_index] = Process(target=start_broker, args=(ZK_CONFIG_PATH, repl_broker.key))
        broker_procs[repl_index].start()
        time.sleep(0.5)
        
        # 5) Kill True Primary 
        broker_procs[true_index].terminate()
        time.sleep(10)
        
        results = psclient.consume(topic, 0)
        print(results)
        self.assertEqual(data, results)

        broker_procs[repl_index].terminate()
        time.sleep(8)

if __name__ == "__main__":
    # if len(sys.argv) != 2:
    #     print("Usage: python src/pubsub_integ_test.py <zk_config>") 
    #     exit(1)
    # ZK_CONFIG_PATH = sys.argv[1]
      
    unittest.main()