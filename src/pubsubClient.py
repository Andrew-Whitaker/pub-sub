from kazoo.client import KazooClient
import xmlrpc.client

class PubSubClient:

    def __init__(self, zk_hosts):
        self.zk_hosts = zk_hosts

    def create_topic(self):
        pass

    def delete_topic(self):
        pass

    def topic_exists(self):
        pass

    def publish(self, topic: str, message: str):
        pass

    def consume(self, topic: str, index: int):
        pass

    #==============================
    # Private Methods
    
    def buildBrokerClient(self, host: str):
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


