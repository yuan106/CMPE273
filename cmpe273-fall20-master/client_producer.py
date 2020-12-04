import zmq
import time
import sys
from itertools import cycle

from ClusterManager import ClusterManager
import consistent_hashing
import hrw

def create_clients(servers):
    producers = {}
    context = zmq.Context()
    for server in servers:
        print(f"Creating a server connection to {server}...")
        producer_conn = context.socket(zmq.REQ)
        producer_conn.bind(server)
        producers[server] = producer_conn
    return producers
    

def generate_data_round_robin(servers):
    print("Starting...")
    producers = create_clients(servers)
    pool = cycle(producers.values())
    for num in range(10):
        data = { 'op': 'PUT', 'key': f'key-{num}', 'value': f'value-{num}' }
        print(f"Sending data:{data}")
        next(pool).send_json(data)
        time.sleep(1)
    print("Done")

def add_node(self, node):
        # Adds a `node` to the hash ring (including a number of replicas).
        for i in xrange(0, self.replicas):
            key = self.gen_key('%s:%s' % (node, i))
            self.keys[key] = node
        self._sorted_keys.append(key)
        self._sorted_keys.sort()

def create_consistent_hashing_ring(servers):
    print("Starting...")
    ## TODO
    producers = create_clients(servers)

    # Create empty ring
    ring = consistent_hashing.ConsistentHashRing()

    # Add nodes to ring
    for nodename, node in producers.items():
        ring[nodename] = node

    return ring

def generate_data_consistent_hashing(ring):
    print("Starting PUT...")
    
    # Send data
    for num in range(10):
        data = { 'op': 'PUT', 'key': f'key-{num}', 'value': f'value-{num}' }
        node = ring[data['key']]
        print(f"Sending data:{data}")
        node.send_json(data)
        node.recv_json()
        time.sleep(1)
    print("Done")
    
def generate_data_hrw_hashing(servers):
    print("Starting...")
    ## TODO
    producers = create_clients(servers)
    seed = 1
    nodes = []

    # Add nodes (all equally weighted)
    for nodename, node in producers.items():
        nodes.append(hrw.Node(node, seed, weight=1))
        seed += 1

    # Send data
    for num in range(10):
        data = { 'op': 'PUT', 'key': f'key-{num}', 'value': f'value-{num}' }
        print(f"Sending data:{data}")
        node = hrw.determine_responsible_node(nodes, data['key'])
        node.node.send_json(data)
        time.sleep(1)
    print("Done")

def get_one_consistent_hashing(ring):
    print("Starting GET_ONE...")
    
    # Send data
    for num in range(10):
        data = { 'op': 'GET_ONE', 'key': f'key-{num}' }
        print(f"Sending data:{data}")
        node = ring[data['key']]
        node.send_json(data)
        resp = node.recv_json()
        print(resp)
        time.sleep(1)
    print("Done")

def get_all_consistent_hashing(ring):
    print("Starting GET_ALL...")

    for nodename, node in ring.nodesByName.items():
        data = { 'op': 'GET_ALL' }
        print(f"Sending data:{data} to {nodename}")
        node.send_json(data)
        resp = node.recv_json()
        print(nodename, resp)
    print("Done")

def addNode(cm, ring, node_id):

    print(f'Adding node {node_id}')

    port = 2000 + node_id

    # Register with Consul
    cm.registerNode(port)

    # Connect to notification server
    # Port 1999 is dedicated to push notifications
    server = 'tcp://127.0.0.1:1999'
    producers = {}
    context = zmq.Context()
    producer_conn = context.socket(zmq.PUSH)
    producer_conn.bind(server)

    # Send push notification to server
    producer_conn.send_json({'op': 'ADD_NODE', 'port': port})

    # Wait 5 seconds
    print('Wait for 5 seconds')
    time.sleep(5)

    # Rebalance data
    print('Rebalancing...')

    ### Get all data from existing nodes
    kv = {}
    for nodename, node in ring.nodesByName.items():
        data = { 'op': 'GET_ALL' }
        node.send_json(data)
        resp = node.recv_json()
        for entry in resp['collection']:
            kv[entry['key']] = entry['value']

    newNodeName = f'tcp://127.0.0.1:{port}'

    # Create new node and add to ring
    newNode = context.socket(zmq.REQ)
    newNode.bind(newNodeName)
    ring[newNodeName] = newNode

    for k, v in kv.items():
        data = { 'op': 'PUT', 'key': k, 'value': v }
        node = ring[k]
        node.send_json(data)
        node.recv_json()
        time.sleep(1)

    # Return new ring
    return newNodeName


def removeNode(cm, ring, nodeName):

    print("Removing node "+ str(nodeName) + '...')

    # Get node by nodeName
    node = ring.nodesByName[nodeName]

    # Get all data from node to be removed
    node.send_json({ 'op': 'GET_ALL' })
    resp = node.recv_json()

    # Delete
    del ring[nodeName]

    # Rebalance
    for entry in resp['collection']:
        data = { 'op': 'PUT', 'key': entry['key'], 'value': entry['value'] }
        node = ring[data['key']]
        print(f"Sending data:{data}")
        node.send_json(data)
        node.recv_json()
        time.sleep(1)
    
    #Send remove signal to Consul
    cm.deregisterNode(nodeName=nodeName)

    print("Done")

if __name__ == "__main__":
    servers = []

    cm = ClusterManager()

    nodes = cm.listNodes()

    for nodeName, node in nodes.items():
        port = node['Port']
        servers.append(f'tcp://127.0.0.1:{port}')
        
    print("Servers:", servers)


    # Create Consistent Hash ring
    ring = create_consistent_hashing_ring(servers)

    # Phase 1 Code
    # generate_data_round_robin(servers)
    # generate_data_consistent_hashing(servers)
    # generate_data_hrw_hashing(servers)

    # Put data into nodes
    generate_data_consistent_hashing(ring)

    # Get each data
    get_one_consistent_hashing(ring)

    # Get all data from each node
    get_all_consistent_hashing(ring)

    # Add node 4
    newServer = addNode(cm, ring, node_id=len(nodes))
    servers.append(newServer)

    get_all_consistent_hashing(ring)
    get_one_consistent_hashing(ring)

    # Remove node 0
    removeNode(cm, ring, nodeName=servers[0])

    get_all_consistent_hashing(ring)
    get_one_consistent_hashing(ring)

    
    
