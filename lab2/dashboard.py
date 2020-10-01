import zmq
# import time
# import pprint

# ZeroMQ Context
context2 = zmq.Context()

# Define the socket using the "Context"
sock = context2.socket(zmq.PULL)
sock.connect("tcp://127.0.0.1:1222")

# Run a simple "Echo" server
while True:
    # Receive data from the socket.
    message= sock.recv()
    message = message.decode()
    reply_msg = "reply_msg: " + message
    # sock.send(reply_msg.encode())
    print(reply_msg)

    

# collecter_data = {}
# for x in xrange(1000):
#     result = results_receiver.recv_json()
#     if collecter_data.has_key(result['worker']):
#         collecter_data[result['worker']] = collecter_data[result['worker']] + 1
#     else:
#         collecter_data[result['worker']] = 1
#     if x == 999:
#         pprint.pprint(collecter_data)