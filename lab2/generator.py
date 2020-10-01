import zmq
import sys
import time

# ZeroMQ Context
context = zmq.Context()

# Define the socket using the "Context"
sock = context.socket(zmq.PUSH)
# socket.bind(address)
# Bind the socket to address
sock.bind("tcp://127.0.0.1:1111")

# Send a "message" using the socket
for num in range(10001):
    time.sleep(1)
    # print (range(10))
    print(num)
    # Syntax : { } .format(value)
    # Returns a formatted string 
    sock.send("{}".format(num).encode())

sock.close()
   
