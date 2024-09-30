import socket
from Presentation import ClientNode


host_name = socket.gethostname() 
ip = socket.gethostbyname(host_name)
client = ClientNode(ip)