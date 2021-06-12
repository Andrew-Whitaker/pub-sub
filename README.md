# pub-sub
Simple Pub Sub implementation with ZooKeeper

## Getting Started

### Install ZooKeeper lcoally on your machine
1. Verify that you have Java installed
2. Follow the instructions: [Getting Started](https://zookeeper.apache.org/doc/current/zookeeperStarted.html) You only need a single server. The only snag you may experience is trying to create the /var/lib/zookeeper folder that ZooKeeper will put its data into. You'll have to make the file with sudo, or you can just change the *dataDir* parameter to any path you want.
3. Start your cluster: ```bin/zkServer.sh start```

* Can stop your cluster with: ```bin/zkServer.sh stop```

### Install kazoo Package for Python
```pip install kazoo```

### Run the small client Python Script
Run ```python src/client.py``` from within the root directory of the git repo.

### Running REPL against Remote Targets

Pass the IPs of all known Zookeepers to the REPL like so:

    python src/client_repl.py 3.84.39.154:2181

### Useful Commands for Running Testing Tools 

Start a broker at a given address:
    
    python src/pubsubBroker.py localhost:3000 3.84.39.154:2181

Run a $n$ consumers for $k$ seconds bulk for testing purposes:
    
    python src/consumers.py n k 3.84.39.154:2181

Run a $n$ publishers for $k$ seconds for bulk testing purposes:
    
    python src/publishers.py n k 3.84.39.154:2181
