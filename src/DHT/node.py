import threading
import socket
import logging

from DHT.chord import ChordNode, ChordNodeReference, getShaRepr
from database_controller.controller_database import DocumentController
from searcher.process_query import Retrieval_Vectorial

# Configurar el nivel de log
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')

logger = logging.getLogger(__name__)

# Operation codes
FIND_SUCCESSOR = 1
FIND_PREDECESSOR = 2
GET_SUCCESSOR = 3
GET_PREDECESSOR = 4
NOTIFY = 5
CHECK_NODE = 6
CLOSEST_PRECEDING_FINGER = 7
STORE_KEY = 8
RETRIEVE_KEY = 9
SEARCH = 10
JOIN = 11
NOTIFY_PRED = 12
GET = 13
INSERT = 14
REMOVE = 15
EDIT = 16


def read_or_create_db(controller):
    connect = controller.connect()
    
    cursor = connect.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS documentos (
        	id INTEGER PRIMARY KEY,
        	text TEXT NOT NULL,
        	tf TEXT
        );
        ''')
        
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS replica_succ (
        	id INTEGER PRIMARY KEY,
        	text TEXT NOT NULL,
        	tf TEXT
        );
        ''')
        
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS replica_pred (
        	id INTEGER PRIMARY KEY,
        	text TEXT NOT NULL,
        	tf TEXT
        );
        ''')
    connect.commit()
    connect.close()
        
class Node(ChordNode):    
    def __init__(self, ip: str, port: int = 8001, m: int = 160):
        super().__init__(ip, port, m)
        self.controller = DocumentController(self.ip)
        self.model = Retrieval_Vectorial()
        read_or_create_db(self.controller)
        threading.Thread(target=self.start_server, daemon=True).start()  # Start server thread

    def add_doc(self,document):
        return self.controller.create_document(id, document)
    
    def upd_doc(self,id,text):
        return self.controller.update_document(id, text)
    
    def del_doc(self,id):
        return self.controller.delete_document(id)
    
    def get_docs(self):
        return self.controller.get_documents()
    
    def get_doc_by_id(self,id):
        return self.controller.get_document_by_id(id)
    
    def search(self, query):
        return self.model.retrieve(query, self.controller)
    
    def data_receive(self, conn: socket, addr, data: list):
        data_resp = None 
        option = int(data[0])
        logger.debug(f'ip {self.ip} recv {option}')

        if option == FIND_SUCCESSOR:
            id = int(data[1])
            data_resp = self.find_succ(id)
                    
        elif option == FIND_PREDECESSOR:
            id = int(data[1])
            data_resp = self.find_pred(id)

        elif option == GET_SUCCESSOR:
            data_resp = self.succ if self.succ else self.ref

        elif option == GET_PREDECESSOR:
            data_resp = self.pred if self.pred else self.ref

        elif option == NOTIFY:
            ip = data[2]
            self.notify(ChordNodeReference(ip, self.port))

        elif option == NOTIFY_PRED:
            ip = data[2]
            self.notify_pred(ChordNodeReference(ip, self.port))

        elif option == CHECK_NODE: data_resp = self.ref

        elif option == CLOSEST_PRECEDING_FINGER:
            id = int(data[1])
            data_resp = self.closest_preceding_finger(id)

        elif option == STORE_KEY:
            key, value = data[1], data[2]
            self.data[key] = value

        elif option == RETRIEVE_KEY:
            key = data[1]
            data_resp = self.data.get(key, '')

        elif option == JOIN and self.id == self.succ.id:
            ip = data[2]
            self.join(ChordNodeReference(ip, self.port))

        elif option == INSERT:
            text = ','.join(data[1:])
            logger.debug(f'\n\nTHE TEXT:\n\n{text}\n\n')
            id = getShaRepr(','.join(data[1:min(len(data),5)]))
            self.add_doc(id, text)

        elif option == GET:
            id = data[1]
            data_resp = self.get_doc_by_id(id)[0]

        elif option == REMOVE:
            id = data[1]
            data_resp = self.del_doc(id)

        elif option == EDIT:
            for i in range(1, len(data)):
                if data[i] == '---':
                    id = ','.join(data[1:i])
                    text = ','.join(data[i+1:])
                    self.upd_doc(id, text)
                    break

        elif option == SEARCH:
            query = ','.join(data[1:])
            response = self.search(query)
            id = response[0][1]
            text = response[0][0][0]
            text = text.split()
            text = text[:min(20, len(text))]
            text = ' '.join(text)
            data_resp = (id, text)
            logger.debug(data_resp)


        if data_resp and option == GET:
            response = data_resp.encode()
            conn.sendall(response)

        elif data_resp and option == SEARCH:
            response = f'{data_resp[0]},{data_resp[1]}'.encode()
            conn.sendall(response)

        elif data_resp:
            response = f'{data_resp.id},{data_resp.ip}'.encode()
            conn.sendall(response)
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

                threading.Thread(target=self.data_receive, args=(conn, addr, data)).start()
                