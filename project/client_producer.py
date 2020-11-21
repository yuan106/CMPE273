import zmq
import time
import sys
from itertools import cycle

import consistent_hashing
import hrw

def create_clients(servers):
    producers = {}
    context = zmq.Context()
    for server in servers:
        print(f"Creating a server connection to {server}...")
        producer_conn = context.socket(zmq.PUSH)
        producer_conn.bind(server)
        producers[server] = producer_conn
    return producers
    

def generate_data_round_robin(servers):
    print("Starting...")
    producers = create_clients(servers)
    pool = cycle(producers.values())
    for num in range(10):
        data = { 'key': f'key-{num}', 'value': f'value-{num}' }
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

def generate_data_consistent_hashing(servers):
    print("Starting...")
    ## TODO
    producers = create_clients(servers)

    # Create empty ring
    ring = consistent_hashing.ConsistentHashRing()

    # Add nodes to ring
    for nodename, node in producers.items():
        ring[nodename] = node
    
    # Send data
    for num in range(10):
        data = { 'key': f'key-{num}', 'value': f'value-{num}' }
        print(f"Sending data:{data}")
        node = ring[data['key']]
        node.send_json(data)
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
        data = { 'key': f'key-{num}', 'value': f'value-{num}' }
        print(f"Sending data:{data}")
        node = hrw.determine_responsible_node(nodes, data['key'])
        node.node.send_json(data)
        time.sleep(1)
    print("Done")

    
if __name__ == "__main__":
    servers = []
    num_server = 1
    if len(sys.argv) > 1:
        num_server = int(sys.argv[1])
        print(f"num_server={num_server}")
        
    for each_server in range(num_server):
        server_port = "200{}".format(each_server)
        servers.append(f'tcp://127.0.0.1:{server_port}')
        
    print("Servers:", servers)
    generate_data_round_robin(servers)
    generate_data_consistent_hashing(servers)
    generate_data_hrw_hashing(servers)
    
