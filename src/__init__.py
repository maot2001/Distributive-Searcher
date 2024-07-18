import socket
import sys
from DHT.node import Node

if __name__ == "__main__":
    
    host_name = socket.gethostname() 
    ip = socket.gethostbyname(host_name)
    node = Node(ip, election=True)

    if len(sys.argv) >= 2:
        node.join_CN()
    while True:
        pass