import threading
import socket
import logging
import time

from queue import Queue
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
#####
CHECK_DOCKS = 17
FIND_LEADER = 18
QUERY_FROM_CLIENT = 20
SEARCH_CLIENT = 26

GET_CLIENT = 21
INSERT_CLIENT = 22
REMOVE_CLIENT = 23
EDIT_CLIENT = 24
SEARCH_CLIENT = 25

SEARCH_SLAVE = 30

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
    def __init__(self, ip: str, port: int = 8001, m: int = 160, election: bool = False, leader_ip = '172.17.0.2',leader_port = 8002):
        super().__init__(ip, port, m, election= election)
        self.controller = DocumentController(self.ip)
        self.model = Retrieval_Vectorial()
        read_or_create_db(self.controller)

        self.is_leader = False
        self.leader_ip = leader_ip
        self.leader_port = leader_port
        threading.Thread(target=self.start_server, daemon=True).start()  # Start server thread
        threading.Thread(target=self.listen_for_broadcast, daemon=True).start()

    def add_doc(self, id, document, table):
        return self.controller.create_document(id, document, table)
    
    def upd_doc(self, id, text, table):
        return self.controller.update_document(id, table, text)
    
    def del_doc(self, id, table):
        return self.controller.delete_document(id, table)
    
    def get_docs(self, table):
        return self.controller.get_documents(table)
    
    def get_doc_by_id(self, id):
        return self.controller.get_document_by_id(id)
    
    def search(self, query):
        return self.model.retrieve(query, self.controller)
    
    def listen_for_broadcast(self):
        broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        broadcast_socket.bind(('', 8002))

        while True:

            msg, client_address = broadcast_socket.recvfrom(1024)
            client_address = client_address.decode('utf-8').split(':')
            msg = msg.decode('utf-8').split(',')
            option = int(msg[0])
            
            if not self.e.ImTheLeader:
                time.sleep(4)
                continue
            if option == FIND_LEADER:
                response = f'{self.e.Leader},{self.leader_port}'.encode()  # Prepara la respuesta con IP y puerto del líder
                broadcast_socket.sendto(response, (client_address[0],8003))  # Envía la respuesta al cliente

              # Envía la respuesta al cliente

    def send_broadcast(self, port, message):
        # Crear un socket UDP
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Permitir reutilización de dirección local
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Habilitar broadcast
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        try:
            # Enviar mensaje por broadcast
            sock.sendto(f'{SEARCH},{message}'.encode('utf-8'), ('<broadcast>', port))
            print(f"Mensaje enviado: {message}")
        finally:
            # Cerrar el socket al finalizar
            sock.close()

    def recv_query_responses(self, responses):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: #Crea el socket "s" con dirección IPv4 (AF_INET) y de tipo TCP (SOCK_STREAM) 
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # SO_REUSEADDR permite reutilizar la dirección local antes que se cierre el socket
            s.bind((self.ip, self.port+1)) #Hace la vinculación de la dirección local de "s"
            s.settimeout(2)
            s.listen(10) # Hay un máximo de 10 conexiones  pendientes

            now = time.time()
            while now - time.time() > 5000:
                try:

                    conn, _ = s.accept() #conexión y dirección del cliente respectivamente
                    data = conn.recv(1024).decode().split(',') # Divide el string del mensaje por las ","


                    logger.debug('/n/n/n')
                    logger.debug(data)
                    logger.debug('/n/n/n')
                    op = data[0]
                    if op == SEARCH_SLAVE:
                        id = data[1]
                        text = ','.join(data[2:])
                        responses.append((id, text))
                except:
                    break 
    
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
            table = data[1]
            id = data[2]
            text = ','.join(data[3:])
            logger.debug(f'\n\nTHE TEXT:\n\n{text}\n\n')
            self.add_doc(id, text, table)
            
            if table == 'documentos':
                if self.pred:
                    self.pred._send_data(INSERT, f'replica_succ,{id},{text}')
                self.succ._send_data(INSERT, f'replica_pred,{id},{text}')

        elif option == GET:
            id = data[1]
            data_resp = self.get_doc_by_id(id)[0]

        elif option == REMOVE:
            table = data[1]
            id = data[2]
            self.del_doc(id, table)
            
            if table == 'documentos':
                if self.pred:
                    self.pred._send_data(REMOVE, f'replica_succ,{id}')
                self.succ._send_data(REMOVE, f'replica_pred,{id}')

        elif option == EDIT:
            table = data[1]
            id = data[2]
            text = ','.join(data[3:])

            logger.debug(f'/n/n id: {id}/ntable: {table}/ntext: {text}/n/n')
            self.upd_doc(id, text, table)
                    
            if table == 'documentos':
                if self.pred:
                    self.pred._send_data(EDIT, f'replica_succ,{id},{text}')
                self.succ._send_data(EDIT, f'replica_pred,{id},{text}')

        elif option == SEARCH:
            query = ','.join(data[1:])
            response = self.search(query)
            
            data_resp = f'{response[0][1]},{response[0][0][0]}'
            logger.debug(data_resp)

        elif option == CHECK_DOCKS:
            self.check_docs_pred()

        elif option == INSERT_CLIENT:
                text = ','.join(data[1:])
                id = getShaRepr(','.join(data[1:min(len(data),5)]))
                logger.debug(f'/n/n/nThe id is {id}/n/n/n')
                node = self.find_succ(id)
                node._send_data(INSERT, f'documentos,{id},{text}')

        elif option == GET_CLIENT:
                id = int(data[1])
                node = self.find_succ(id)
                response = node._send_data(GET, f'{id}')
                response = f'{id},{response}'
                data_resp = response

        elif option == REMOVE_CLIENT:
                id = int(data[1])
                node = self.find_succ(id)
                node._send_data(REMOVE, f'documentos,{id}')

        elif option == EDIT_CLIENT:
                id = int(data[1])
                text = ','.join(data[2:])
                node._send_data(EDIT, f'documentos,{id},{text}')

        elif option == SEARCH_CLIENT:
                query = ','.join(data[1:])
                responses = []
                recv = threading.Thread(target=self.recv_query_responses, args=(responses,))
                recv.start()
                self.send_broadcast(8001, query)
                recv.join()
                
                data_resp = '&&&'.join(f'{i[0]},{i[1]}' for i in responses)
                data_resp = responses



        
        if data_resp and (option == GET_CLIENT or option == SEARCH_CLIENT):
            response = data_resp.encode()
            conn.sendall(response)


        if data_resp and option == GET:
            response = data_resp.encode()
            conn.sendall(response)

        elif data_resp and option == SEARCH:
            response = data_resp.encode()
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
      
    
    def notify(self, node: 'ChordNodeReference'):
        super().notify(node)
        self.check_docs()
        self.pred._send_data(CHECK_DOCKS)
    
    def get_docs_between(self, tables, min, max):
        return self.controller.get_docs_between(tables, min, max)


    # Reciev boradcast message
    def _reciev_broadcast(self):
        # #logger.debug("recive broadcast de chord")
        

        while True:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            # s.bind(('', int(self.port)))
            s.bind(('', int(self.port)))
            # #logger.debug(f"recive broadcast de chord in while por el puerto {self.port}!!!")
            
            msg, addr = s.recvfrom(1024)

            # #logger.debug(f'Received broadcast: {self.ip}')

            msg = msg.decode().split(',')
            # #logger.debug(f'received broadcast msg: {msg}')
            # #logger.debug(f"recive broadcast de chord in while before try!!!")
            # try:
                
            option = int(msg[0])
            # #logger.debug(f"recive broadcast de chord option {option}")
            # if option == REQUEST_BROADCAST_QUERY:
            #     hashed_query, query = msg[1].split(',', 1)  # Asume que el mensaje recibido tiene la forma: hash,query
        
            #     # Verifica si el mensaje es una respuesta a nuestra consulta actual
            #     if hasattr(self, None) and self.hash_query == hashed_query:
            #         # Pone la respuesta en la cola de respuestas del Leader
            #         self.responses_queue.put(query)
            
            if option == JOIN:
                # msg[2] es el ip del nodo
                if msg[2] == self.ip:
                    #logger.debug(f'My own broadcast msg: {self.id}')
                    continue
                else:
                    # self.ref._send_data(JOIN, {self.ref})
                    try:
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                            # #logger.debug(f'_send_data: {self.ip}')
                            s.connect((msg[2], self.port))
                            s.sendall(f'{JOIN},{self.ref}'.encode('utf-8'))
                            # #logger.debug(f'_send_data end: {self.ip}')
                            continue
                    except Exception as e:
                        #logger.debug(f"Error sending data: {e}")
                        continue
                #TODO Enviar respuesta
            
            # if option == FIND_LEADER:
            #     # print("Entra al if correcto en chord")
            #     # Asegúrate de que msg[1] contiene la dirección IP del cliente que hizo el broadcast
            #     ip_client = msg[1].strip()  # Elimina espacios en blanco
            #     response = f'{self.ip},{self.port}'.encode()  # Prepara la respuesta con IP y puerto del líder
            #     # print("-----------------------------------------")
            #     # print(f"enviando respuesta {response} a {(ip_client,8003)}")
            #     # print("-----------------------------------------")
            #     s.sendto(response, (ip_client,8003))  # Envía la respuesta al cliente
            if option == SEARCH:
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                            # #logger.debug(f'_send_data: {self.ip}')

                        logger.debug(f'\n\n\nI am the slave reciving {query}\n\n\n')
                        query = ','.join(msg[1:])
                        response = f"{SEARCH_SLAVE},{self.search(query)}".encode()
                        addr = addr.split(':')
                        s.connect((addr[0], 8002))
                        s.sendall(response)
                            # #logger.debug(f'_send_data end: {self.ip}')
                        # self.send_broadcast(8002,response)
        
                        # #logger.debug(f'_send_data: {self.ip}')
                        # s.connect(('<broadcast>', 8002))
                        # s.sendall()
                        # #logger.debug(f'_send_data end: {self.ip}')
                        continue
                except Exception as e:
                    #logger.debug(f"Error sending data: {e}")
                    continue
                
            if option == FIND_LEADER and self.e.ImTheLeader:
                response = f'{self.e.Leader},{self.leader_port}'.encode()  # Prepara la respuesta con IP y puerto del líder
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                            # #logger.debug(f'_send_data: {self.ip}')
                            s.connect((msg[2], 8002))
                            s.sendall(f'{JOIN},{self.ref}'.encode('utf-8'))
                            # #logger.debug(f'_send_data end: {self.ip}')
                            continue  # Envía la respuesta al cliente
            
            # except Exception as e:
            #     print(f"Error in _receiver_boradcast: {e}")

    
            
    def start_server_to_receive_responses(self, port, response_queue):
        """
        Inicia un servidor temporal en el puerto especificado para recibir respuestas.
        Las respuestas recibidas se agregan a la cola dada.
        """
        try:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.settimeout(2)
            server_socket.bind(('', port))
            print("/////////////////")
            print("///////BEFORE MESSAGE//////////")
            print("/////////////////")
            
            now = time.time()
            while time.time() - now <= 5000:
                try:
                    print(time.time() - now)
                    message, addr = server_socket.recvfrom(1024)
                    message = message.decode()
                    message = message.split(",")
                    text = (",").join(message[1:])
                    option = int(message[0])
                    text = message[1]

                    print(f"//////DEL NODO PA LA COLA {option}////////")
                    print(f"//////{text}///////")
                    print("/////////////////")
                    if option == SEARCH:
                        response_queue.put(text)
                except:
                    break
                
                
        except Exception:
            print("Exploto esta talla")
        finally:
            server_socket.close()
                
    def check_docs(self):

        # toma sus documentos y las replicas de su predecesor
        my_docs = self.get_docs('documentos')
        pred_docs = self.get_docs('replica_pred')

        for doc in my_docs:
            # si el id NO esta entre su nuevo predecesor y el, o sea le pertenece a su predecesor
            if not self._inbetween(doc[0], self.pred.id, self.id):
                
                # le dice que lo inserte en sus documentos
                self.pred._send_data(INSERT, f'documentos,{doc[0]},{doc[1]}')
                
                # lo elimina de sus documentos
                self.del_doc(doc[0], 'documentos')
                self.succ._send_data(REMOVE, f'replica_pred,{doc[0]}')
            
            else:
                # esta entre los 2, asi que le pertenece al sucesor y le notifica que lo replique
                if self.pred:
                    self.pred._send_data(INSERT, f'replica_succ,{doc[0]},{doc[1]}')

        
        for doc in pred_docs:
            # si el id NO esta entre su nuevo predecesor y el, o sea le pertenece al antiguo predecesor
            if not self._inbetween(doc[0], self.pred.id, self.id):
                
                # le dice que lo replique como documento del que ahora es el predecesor del nuevo predecesor
                if self.pred:
                    self.pred._send_data(INSERT, f'replica_pred,{doc[0]},{doc[1]}')

                # lo elimina porque cambio su predecesor
                self.del_doc(doc[0], 'replica_pred')

            else:
                # si el id esta entre su nuevo predecesor y el, o sea le pertenece a el
                self.add_doc(doc[0], doc[1], 'documentos')

                # luego lo elimina de sus replicados
                self.del_doc(doc[0], 'replica_pred')

                # despues lo mandan a replicar
                if self.pred:
                    self.pred._send_data(INSERT, f'replica_succ,{doc[0]},{doc[1]}')
                self.succ._send_data(INSERT, f'replica_pred,{doc[0]},{doc[1]}')

    
    
    # luego aqui entra el predecesor
    def check_docs_pred(self):
        
        # toma sus documentos y las replicas de su sucesor
        my_docs = self.get_docs('documentos')
        succ_docs = self.get_docs('replica_succ')

        for doc in my_docs:
           
            # los documentos que me pertenecen los replico a mi nuevo sucesor
            self.succ._send_data(INSERT, f'replica_pred,{doc[0]},{doc[1]}')


        for doc in succ_docs:
            # si el id NO esta entre su nuevo sucesor y el, o sea le pertenece al antiguo sucesor
            if not self._inbetween(doc[0], self.id, self.succ.id):

                # le dice que lo replique como documento del que ahora es el sucesor del nuevo sucesor
                self.succ._send_data(INSERT, f'replica_succ,{doc[0]},{doc[1]}')
                
                # lo elimina porque cambio su sucesor
                self.del_doc(doc[0], 'replica_succ')
            
            else:
                # si el id esta entre su nuevo sucesor y el, o sea le pertenece al nuevo sucesor
                self.succ._send_data(INSERT, f'documentos,{doc[0]},{doc[1]}')