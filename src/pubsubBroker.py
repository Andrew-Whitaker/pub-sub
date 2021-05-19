from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn


class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class threadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass


class PubSubBroker:

    def __init__(self, zk_hosts):
        self.zk_hosts = zk_hosts
    
    def enqueue(topic: str, message: str):
        pass

    def enqueue_replica(topic: str, message: str, index: int):
        pass

    def last_index(topic: str):
        pass

    def consume(topic: str, index: int):
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
    broker = PubSubBroker(zk_hosts)

    # Register all functions in the Broker's Public API
    rpc_server.register_introspection_functions()
    rpc_server.register_function(broker.enqueue, "broker.enqueue")
    rpc_server.register_function(broker.enqueue_replica, "broker.enqueue_replica")
    rpc_server.register_function(broker.last_index, "broker.last_index")
    rpc_server.register_function(broker.consume, "broker.consume")

    print("Started successfully.")
    print("Accepting requests. (Halt program to stop.)")

    # Start Broker Server
    rpc_server.serve_forever()
