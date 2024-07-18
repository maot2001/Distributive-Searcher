import socket
import time
import streamlit as st
import threading
from streamlit.runtime.scriptrunner import add_script_run_ctx

from DHT.chord import ChordNode, ChordNodeReference, getShaRepr

JOIN = 11
GET_CLIENT = 21
INSERT_CLIENT = 22
REMOVE_CLIENT = 23
EDIT_CLIENT = 24
FIND_LEADER = 18

import logging

# Configurar el nivel de log
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')

logger = logging.getLogger(__name__)

class ClientNode(ChordNode):
    def __init__(self, ip: str, port: int = 8001):
        self.id = getShaRepr(ip)
        self.ip = ip
        self.port = port
        self.ref = ChordNodeReference(self.ip, self.port)
        self.connect = None
        t = threading.Thread(target=self.start_server, daemon=True)  # Start server thread
        add_script_run_ctx(t)
        t.start()

        logger.debug('thread')

        self.join_CN(self.ref)
        logger.debug('join')
        
        while self.connect is None:
            time.sleep(2)        
        index(self)
        logger.debug('index')

    def data_receive(self, conn: socket, addr, data: list):
        option = int(data[0])

        if option == JOIN and self.connect is None:
            ip = data[2]
            self.connect = ChordNodeReference(ip)
            logger.debug(f'connect to {ip}')

        conn.close()

    #region 
    # Start server method to handle incoming requests
    def start_server(self):

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: #Crea el socket "s" con dirección IPv4 (AF_INET) y de tipo TCP (SOCK_STREAM) 
            
            #*Más configuración del socket "s"  
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # SO_REUSEADDR permite reutilizar la dirección local antes que se cierre el socket
            #? La configuración anterior nos conviene?
             
            s.bind((self.ip, 8002)) #Hace la vinculación de la dirección local de "s"
            
            s.listen(10) # Hay un máximo de 10 conexiones  pendientes

            while True:
                
                conn, addr = s.accept() #conexión y dirección del cliente respectivamente
                
                logger.debug(f'new connection from {addr}')

                data = conn.recv(1024).decode().split(',') # Divide el string del mensaje por las ","

                t2 = threading.Thread(target=self.data_receive, args=(conn, addr, data))
                add_script_run_ctx(t2)
                t2.start()

    def join_CN(self, ip):
        logger.debug(f'Broadcast: {self.ip}')
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.sendto(f'{FIND_LEADER}, {ip}'.encode(), (str(socket.INADDR_BROADCAST), self.port))
        logger.debug(f'Broadcast end: {self.ip}')
        s.close()

def index(client):
    if "client" not in st.session_state:
        st.session_state.client = client

    if 'query' in st.session_state:
        del st.session_state['query']

    if 'doc' in st.session_state:
        del st.session_state['doc']

    st.title('Bienvenido a nuestro proyecto de Sistemas Distribuidos')
    st.subheader(
    "Sientese, disfrute y sea amable con el proyecto ;)", divider=True
)

    st.page_link("pages/Home.py", label="**Dis-Gle**", icon="📚")


host_name = socket.gethostname() 
ip = socket.gethostbyname(host_name)
client = ClientNode(ip)