import os
import shutil
import socket
import sys
from DHT.node import Node

if __name__ == "__main__":

    if len(sys.argv) < 2:
        for f in os.listdir('src/data'):
            shutil.rmtree(os.path.join('src/data', f))
    
    host_name = socket.gethostname() 
    ip = socket.gethostbyname(host_name)
    node = Node(ip, election=True)
    node.time_assign()

    if len(sys.argv) >= 2:
        node.time_assign(int(sys.argv[1]))
        node.join_CN()

    while True:
        pass