import os
import socket
import time
from joblib import load, dump
import streamlit as st
import threading
from streamlit.runtime.scriptrunner import add_script_run_ctx

from DHT.chord import ChordNode, ChordNodeReference, getShaRepr

SEARCH = 10
JOIN = 11
GET = 13
INSERT = 14
REMOVE = 15
EDIT = 16

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
        self.index()
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
             
            s.bind((self.ip, self.port)) #Hace la vinculación de la dirección local de "s"
            
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
        s.sendto(f'{JOIN}, {ip}'.encode(), (str(socket.INADDR_BROADCAST), 8001))
        logger.debug(f'Broadcast end: {self.ip}')
        s.close()

    def index(self):
        uploaded_file = st.sidebar.file_uploader("Subir archivo .txt", type=['txt'])

        if uploaded_file is not None:
            self.connect._send_data(INSERT, uploaded_file.getvalue().decode())
            uploaded_file = None

        st.title('Dis-Gle')
        with st.form(key='search_form'):
            text = st.text_input("Buscar:", "")
            submit_button = st.form_submit_button(label="Buscar")

        if submit_button:
            dump(text, f'query:{self.ip}.joblib')

    def search(self, id, doc):
        st.title('Dis-Gle')
        with st.form(key='search_form'):
            text = st.text_input("Buscar:", "")
            submit_button = st.form_submit_button(label="Buscar")

        if submit_button:
            dump(text, f'query:{self.ip}.joblib')
            if os.path.exists(f'doc:{self.ip}.joblib'): os.remove(f'doc:{self.ip}.joblib')
            
        st.write(doc)
        if st.button('Ver Más...'):
            os.remove(f'query:{self.ip}.joblib')
            dump(id, f'doc:{self.ip}.joblib')


    def text_page(self, id, text):
        st.title('Dis-Gle')

        edit_text = st.text_input(text)
        if st.button('Editar'):
            self.edit_doc(edit_text, id)
            os.remove(f'doc:{self.ip}.joblib')

        if st.button('Eliminar'):
            self.delete_doc(id)
            os.remove(f'doc:{self.ip}.joblib')

    def edit_doc(self, edit_text, id):
        self.connect._send_data(EDIT, f'{id},---,{edit_text}')

    def delete_doc(self, id):
        self.connect._send_data(REMOVE, id)

host_name = socket.gethostname() 
ip = socket.gethostbyname(host_name)
try:
    client = load(f'client:{ip}.joblib')
    try:
        query = load(f'query:{ip}.joblib')
        responses = client.connect._send_data(SEARCH, query)
        data = responses.decode().split(',')
        id = data[0]
        text = ','.join(data[1:])
        logger.debug(id)
        logger.debug(text)
        client.search(id, text)
    except:
        try:
            doc = load(f'doc:{ip}.joblib')
            response = client.connect._send_data(GET, doc)
            data = response.decode().split(',')
            text = ','.join(data[0:])
            logger.debug(doc)
            logger.debug(text)
            client.text_page(doc, text)
        except:
            client.index()
except:
    client = ClientNode(ip)
    dump(client, f'client:{ip}.joblib')
