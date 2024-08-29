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
SEARCH_CLIENT = 25

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

    def check(self):
        while self.connect is None or self.connect.check_node() == b'':
            self.connect = None
            self.join_CN(self.ref)
            time.sleep(2)

    def search(self, query):
        self.check()
        return self.connect._send_data(SEARCH_CLIENT, query)
    
    def get(self, id):
        self.check()
        return self.connect._send_data(GET_CLIENT, id)

    def insert(self, text):
        logger.debug('check')
        self.check()
        logger.debug('checked')
        self.connect._send_data(INSERT_CLIENT, text)
    
    def edit(self, id, text):
        self.check()
        self.connect._send_data(EDIT_CLIENT, f'{id},{text}')
    
    def remove(self, id):
        self.check()
        self.connect._send_data(REMOVE_CLIENT, id)

    def data_receive(self, conn: socket, addr, data: list):
        option = int(data[0])

        if option == JOIN and self.connect is None:
            ip = data[2]
            self.connect = ChordNodeReference(ip)
            logger.debug(f'connect to {ip}')

        conn.close()

    def start_server(self):

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: 

            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
            s.bind((self.ip, self.port)) 
            s.listen(10) 

            while True:
                conn, addr = s.accept()
                data = conn.recv(1024).decode().split(',')

                t2 = threading.Thread(target=self.data_receive, args=(conn, addr, data))
                add_script_run_ctx(t2)
                t2.start()

    def join_CN(self, ip):
        logger.debug(f'Broadcast: {self.ip}')
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.sendto(f'{JOIN},{ip}'.encode(), (str(socket.INADDR_BROADCAST), 8003))
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