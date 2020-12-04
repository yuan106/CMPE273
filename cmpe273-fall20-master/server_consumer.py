import zmq
import sys
from  multiprocessing import Process

from ClusterManager import ClusterManager

kv = {}

def pullNotifications(cm, port):
    context = zmq.Context()
    consumer = context.socket(zmq.PULL)
    consumer.connect(f"tcp://127.0.0.1:{port}")

    while True:
        raw = consumer.recv_json()
        if raw['op'] == 'ADD_NODE':
            server_port = raw['port']
            cm.registerNode(server_port)
            print(f"Starting a server at:{server_port}...")
            Process(target=server, args=(server_port,)).start()

def server(port):
    context = zmq.Context()
    consumer = context.socket(zmq.REP)
    consumer.connect(f"tcp://127.0.0.1:{port}")
    
    while True:
        raw = consumer.recv_json()
        op = raw['op']
        
        # FIXME: Implement to store the key-value data.
        if op == 'PUT':
            key, value = raw['key'], raw['value']
            print(f"Server_port={port}:key={key},value={value}")
            kv[key] = value
            resp = {}
        elif op == 'GET_ONE':

            key = raw['key']

            if key in kv:
                resp = {
                    "key": key,
                    "value": kv[key]
                }
            else:
                resp = { "message": "Invalid key" }
        elif op == 'GET_ALL':

            collection = []

            for k, v in kv.items():
                collection.append({
                    "key": k,
                    "value": v
                })

            resp = { "collection": collection }
        else:
            resp = { "message": "Invalid op" }

        consumer.send_json(resp)
    
        
if __name__ == "__main__":

    cm = ClusterManager()

    num_server = 1
    if len(sys.argv) > 1:
        num_server = int(sys.argv[1])
        print(f"num_server={num_server}")

    # Listen to add node notifications
    Process(target=pullNotifications, args=(cm, 1999,)).start()
        
    for node_id in range(num_server):
        server_port = 2000 + node_id
        cm.registerNode(server_port)
        print(f"Starting a server at:{server_port}...")
        Process(target=server, args=(server_port,)).start()
    