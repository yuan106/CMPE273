import time
import zmq
import math

while True: 
    # ZeroMQ Context
    context = zmq.Context()
    # receive work, worker_receiver is a socket
    receiver = context.socket(zmq.PULL)
    receiver.connect("tcp://127.0.0.1:1111")
    # Receive data from the socket.
    message = receiver.recv()
    # close the socket
    receiver.close()
    # decode the data
    message = message.decode()
    if message:
        # send work
        sender = context.socket(zmq.PUSH)
        sender.bind("tcp://127.0.0.1:1222")
        work = "the square root of {} is {}".format(message,math.sqrt(int(message)))
        print (work)
        sender.send(work.encode())
        sender.close()

    # python worker.py >worker3.txt
