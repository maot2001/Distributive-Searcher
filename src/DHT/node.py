import threading
import socket
import logging
import time

from DHT.chord import ChordNode, ChordNodeReference, getShaRepr, decode_response
from database_controller.controller_database import DocumentController
from searcher.process_query import Retrieval_Vectorial

# Configurar el nivel de log
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')

logger = logging.getLogger(__name__)

def clockToList(clock: str) -> list[int]:
    return [int(num) for num in clock.strip('[]').split(',') if num.strip()]

def CompareClocks(clock1, clock2):
    try: #Devuelve True si el primero es "más reciente"
        clock1= clockToList(clock1)
        clock2= clockToList(clock2)
        ans = True
        for i in range(0,len(clock1)):
            if clock1[i] >= clock2[i]:
                continue
            else:
                answ = False
                break
        return ans
    except:
            pass

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

GET_CLIENT = 21
INSERT_CLIENT = 22
REMOVE_CLIENT = 23
EDIT_CLIENT = 24
SEARCH_CLIENT = 25

SEARCH_SLAVE = 30
GIVE_TIME = 31
NEW_JOIN = 32
OWNER = 33
PING = 34

def create_db(controller):
    connect = controller.connect()
    
    cursor = connect.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS documentos (
        	id INTEGER PRIMARY KEY,
        	text TEXT NOT NULL,
            clock TEXT NOT NULL,
        	tf TEXT
        );
        ''')
        
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS replica_succ (
        	id INTEGER PRIMARY KEY,
        	text TEXT NOT NULL,
            clock TEXT NOT NULL,
        	tf TEXT
        );
        ''')
        
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS replica_pred (
        	id INTEGER PRIMARY KEY,
        	text TEXT NOT NULL,
            clock TEXT NOT NULL,
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
        create_db(self.controller)

        self.is_leader = False
        self.leader_ip = leader_ip
        self.leader_port = leader_port
        self.find_nodes = set([])
        self.loading = False
        threading.Thread(target=self.start_server, daemon=True).start()  # Start server thread
        threading.Thread(target=self.recv_8003, daemon=True).start()
        threading.Thread(target=self.rings_union, daemon=True).start()
        threading.Thread(target=self.consistency, daemon=True).start()

    def time_assign(self, waiter = 22):
        self.waiter = waiter

    def add_doc(self, id, document, clock, table):
        self.clock.increment()
        return self.controller.create_document(id, document, clock, table)
    
    def upd_doc(self, id, clock, text, table):
        return self.controller.update_document(id, clock, table, text)
    
    def del_doc(self, id, table):
        return self.controller.delete_document(id, table)
    
    def get_docs(self, table):
        return self.controller.get_documents(table)
    
    def get_doc_by_id(self, id, table = "documentos"):
        return self.controller.get_document_by_id(id, table)
    
    def search(self, query):
        return self.model.retrieve(query, self.controller)
    
    def consistency(self):
        while True:
            try:
                while self.loading:
                    logger.debug('Consistency Loading...')
                    time.sleep(10)

                self.loading = True
                logger.debug('Consistency Clear')

                my_docs = self.get_docs('documentos')
                pred_docs = self.get_docs('replica_pred')
                succ_docs = self.get_docs('replica_succ')

                for doc in my_docs:
                    if self.pred:
                        if not self._inbetween(doc[0] // 8192, self.pred.id, self.id):
                            owner = self.find_succ(doc[0] // 8192)
                            clock_copy1 = self.clock.send_event()
                            owner._send_data(INSERT, f'documentos,{doc[0]},{doc[1]},|||,{doc[2]},|||', clock=clock_copy1) # Este paso asegura la replicación a ambos lados del vecino
                            
                            clock_copy2 = self.clock.send_event()
                            self.ref._send_data(REMOVE, f'documentos,{doc[0]}', clock=clock_copy2)
                        else:
                            clock_copy1 = self.clock.send_event()
                            self.succ._send_data(INSERT, f'replica_pred,{doc[0]},{doc[1]},|||,{doc[2]},|||', clock=clock_copy1)
                            
                            clock_copy2 = self.clock.send_event()
                            self.pred._send_data(INSERT, f'replica_succ,{doc[0]},{doc[1]},|||,{doc[2]},|||', clock=clock_copy2)


                for doc in pred_docs:
                    if self.pred:
                        clock_copy1 = self.clock.send_event()
                        response = self.pred._send_data(OWNER, str(doc[0]), clock_copy1).decode()
                        response = True if decode_response(response, split_char=',', char='¬')[0] == 'True' else False
                        if not response:
                            owner = self.find_succ(doc[0] // 8192)
                            clock_copy2 = self.clock.send_event()
                            owner._send_data(INSERT, f'documentos,{doc[0]},{doc[1]},|||,{doc[2]},|||', clock=clock_copy2) # Este paso asegura la replicación a ambos lados del vecino
                            
                            self.del_doc(doc[0], 'replica_pred')


                for doc in succ_docs:
                    clock_copy1 = self.clock.send_event()
                    response = self.succ._send_data(OWNER, str(doc[0]), clock_copy1).decode()
                    response = True if decode_response(response, split_char=',', char='¬')[0] == 'True' else False
                    # logger.debug(f'\n\nto id {doc[0]} my succ {self.succ.id} has {response}\n\n')
                    if not response:
                            owner = self.find_succ(doc[0] // 8192)
                            clock_copy2 = self.clock.send_event()
                            owner._send_data(INSERT, f'documentos,{doc[0]},{doc[1]},|||,{doc[2]},|||', clock=clock_copy2) # Este paso asegura la replicación a ambos lados del vecino
                            
                            self.del_doc(doc[0], 'replica_succ')

                self.loading = False
                logger.debug('Consistency End')
                time.sleep(60)

            except:
                self.loading = False
                logger.debug('Consistency Failed')
                time.sleep(60)

    def rings_union(self):
        while True:
            if not self.e.InElection and self.e.ImTheLeader:

                logger.debug('\n\nSending PING...\n\n')
                self.ref._send_data_global(PING)
                time.sleep(10)

                logger.debug(f'\n\nIn Union {self.find_nodes}\n\n')
                node = self
                if self.ip in self.find_nodes: self.find_nodes.remove(self.ip)

                while True:
                    if node.succ.check_node() == b'':
                        # logger.debug(f'\n\nnot founded {node.succ.ip}\n\n')
                        if node.succ.ip in self.find_nodes: self.find_nodes.remove(node.succ.ip)
                        time.sleep(15)
                    else:
                        node = node.succ
                        break

                while True:
                    if node.check_node() == b'':
                        # logger.debug(f'\n\nnot founded {node.ip}\n\n')
                        break
                    else:
                        if node.ip in self.find_nodes: self.find_nodes.remove(node.ip)
                        # logger.debug(f'\n\ndeleted {node.ip}\n\n')

                        node = node.succ()[0]
                        # logger.debug(f'\n\nchanged to {node.ip}\n\n')

                    if node.ip == self.ip:
                        # logger.debug('\n\nEnd Union\n\n')
                        break

                if len(self.find_nodes) > 0:
                    logger.debug(f'\n\nStill Union {self.find_nodes}\n\n')
                    losted = ChordNodeReference((self.find_nodes.pop()))
                    losted._send_data(NEW_JOIN, str(self.ref))
                        
                self.find_nodes.clear()

                logger.debug(f'\n\nComplete Union\n\n')
            
            time.sleep(30)
    
    def recv_8003(self):
        while True:
            if not self.e.InElection and self.e.ImTheLeader:
                logger.debug('I am the Leader')
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

                s.bind(('', 8003))

                while True:
                    try:
                        msg, _ = s.recvfrom(1024)
                        if not self.e.InElection and not self.e.ImTheLeader:
                            break

                        msg = msg.decode("utf-8").split(',')
                        op = int(msg[0])

                        threading.Thread(target=self.data_receive_8003, args=(op, msg)).start()

                    except Exception as e:
                        logger.debug(f"Error in recv_8003: {e}")
            else:
                time.sleep(3)

    def data_receive_8003(self, op, data):
        if op == JOIN:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((data[2], 8001))
                    s.sendall(f'{JOIN},{self.ref}'.encode('utf-8'))
            except Exception as e:
                logger.debug(f"Error in data_receive_8003: {e}")

    def recv_query_responses(self, responses):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: 
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
            s.bind((self.ip, 8002)) 
            s.settimeout(self.waiter)
            s.listen(10) 

            now = time.time()
            while time.time() - now < self.waiter:
                try:
                    msg, _ = s.accept()

                    data = msg.recv(32768).decode()
                    data = decode_response(data, split_char='$$$', char='¬')

                    clock_sent = data[-1]
                    self.clock.update(clock_sent)
                    data = '$$$'.join(data[:-1]) # Descarto el reloj
                    print(f'without clock {data}')

                    threading.Thread(target=self.process_query_responses, args=(data, responses)).start()
                except:
                    break

    def process_query_responses(self, data, responses):
        ind = data.index(',')
        op = int(data[:ind])
        text = data[ind+1:]
        if text == ',': return
        if op == SEARCH_SLAVE:
            text = text.split('$$$')
            for doc in text:
                if not doc in responses: responses.append(doc)
    
    def data_receive(self, conn: socket, addr, data: list):
        data_resp = None 
        option = int(data[0])

        #logger.debug(f"data receive init = {option} {data[1]} {watch_clocks}")

        # El cliente no envian reloj pero si se verá reflejado en el reloj del "líder"
        if option == GET_CLIENT or option == SEARCH_CLIENT or option == INSERT_CLIENT or option == REMOVE_CLIENT or option == EDIT_CLIENT:
           #logger.debug(f"Petición del cliente! ")
           self.clock.increment()
        else: # Al no ser un mensaje del cliente este tiene reloj
            clock_sent = data[-1]
            #logger.debug(f"Reloj enviado = {clock_sent}")
            data = data[0:-1] # Remover el reloj
            self.clock.update(clock_sent)
        #logger.debug(f"Mi reloj = {self.clock}")


        if option == FIND_SUCCESSOR:
            id = int(data[1])
            # logger.debug(f'find successor to {id}')
            data_resp = self.find_succ(id)
            # logger.debug(f'successor to {id} is {data_resp}')
                    
        elif option == FIND_PREDECESSOR:
            id = int(data[1])
            # logger.debug(f'find predecessor to {id}')
            data_resp = self.find_pred(id)
            # logger.debug(f'predecessor to {id} is {data_resp}')

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

        elif option == JOIN and not self.included:
            self.included = True
            ip = data[2]
            # logger.debug(f'join to {ip}')
            self.join(ChordNodeReference(ip, self.port))

        elif option == NEW_JOIN:
            ip = data[2]
            logger.debug(f'new_join to {ip}')
            self.join(ChordNodeReference(ip, self.port))

        elif option == OWNER:
            id = int(data[1])
            doc = self.get_doc_by_id(id)
            if not doc is None: data_resp = 'True'.encode()
            else: data_resp = 'False'.encode()

        elif option == PING:
            # logger.debug(f'receiving PING from {addr[0]}')
            self.find_nodes.add(addr[0])

        elif option == INSERT:
                id = int(data[2])
                
                index = data.index("|||")
                clock_for_doc = "[" +('\''.join(data[index+2:-2])).replace("\'",",") + "]"
                text = ','.join(data[3:index])
                send_flag = True
                if data[1].startswith("documentos"):
                    table = 'documentos'
                    doc_in_bd = self.get_doc_by_id(id)
                    if not doc_in_bd: # Revisa si esta en la base de datos
                        self.add_doc(id, text, clock_for_doc, table) 
                    else: 
                        clock_in_bd = doc_in_bd[1]
                        if CompareClocks(clock_in_bd,clock_in_bd): # Revisa si el documento guardado en la base de datos es el más reciente
                            send_flag = False # no lo replica se trata de una versión antigua
                        else: 
                            self.add_doc(id, text, clock_for_doc, table)
                    if send_flag:
                        clock_copy = self.clock.send_event()
                        if data[1][len("documentos"):] == "S":
                            if self.pred:
                                self.pred._send_data(INSERT, f'replica_succ,{id},{text},|||,{clock_for_doc},|||', clock=clock_copy)
                        elif data[1][len("documentos"):] == "P":
                            self.succ._send_data(INSERT, f'replica_pred,{id},{text},|||,{clock_for_doc},|||', clock =clock_copy)
                        else:
                            if self.pred:
                                self.pred._send_data(INSERT, f'replica_succ,{id},{text},|||,{clock_for_doc},|||', clock=clock_copy)
                            self.succ._send_data(INSERT, f'replica_pred,{id},{text},|||,{clock_for_doc},|||', clock =clock_copy)
                else: # El documento se guarda en una de las réplicas 
                    if data[1].startswith("replica_succ"):
                        doc_in_bd = self.get_doc_by_id(id,"replica_succ")
                        table = 'replica_succ'
                        if not doc_in_bd: # Devolvió None
                            self.add_doc(id,  text, clock_for_doc, table)
                        else:
                            clock_in_bd = doc_in_bd[1]
                            if not CompareClocks(clock_in_bd,clock_in_bd): # El documento que esta guardado es más viejo
                                self.add_doc(id,  text, clock_for_doc, table)
                                if self.pred:
                                    clock_copy = self.clock.send_event()
                                    self.pred._send_data(INSERT, f'documentosS,{id},{text},|||,{clock_for_doc},|||', clock=clock_copy)
                    else:
                        doc_in_bd = self.get_doc_by_id(id,"replica_pred")
                        table = 'replica_pred'
                        if not doc_in_bd: # Devolvió None
                            self.add_doc(id,  text, clock_for_doc, table)
                        else:
                            clock_in_bd = doc_in_bd[1]
                            if not CompareClocks(clock_in_bd,clock_in_bd): # El documento que esta guardado es más viejo
                                self.add_doc(id,  text, clock_for_doc, table)
                                clock_copy = self.clock.send_event()
                                self.succ._send_data(INSERT, f'documentosP,{id},{text},|||,{clock_for_doc},|||', clock=clock_copy)
                

        elif option == GET:
            id = data[1]
            doc = self.get_doc_by_id(id)[0]
            if doc is None:
                doc = 'None'
            data_resp = doc.encode()

        elif option == REMOVE:
            table = data[1]
            id = data[2]
            self.del_doc(id, table)
            
            if table == 'documentos':
                clock_copy = self.clock.send_event()
                if self.pred:
                    self.pred._send_data(REMOVE, f'replica_succ,{id}', clock= clock_copy)
                self.succ._send_data(REMOVE, f'replica_pred,{id}', clock= clock_copy )

        elif option == EDIT:
            table = data[1]
            id = data[2]
            index = data.index("|||")
            clock_for_doc = ','.join(data[index+1:-1])
            text = ','.join(data[3:index]) 
            self.upd_doc(id, clock_for_doc, text, table)
                    
            if table == 'documentos':
                clock_copy = self.clock.send_event()
                if self.pred:
                    self.pred._send_data(EDIT, f'replica_succ,{id},{text},|||,{clock_for_doc},|||', clock = clock_copy)
                self.succ._send_data(EDIT, f'replica_pred,{id},{text},|||,{clock_for_doc},|||', clock=clock_copy)

        elif option == CHECK_DOCKS:
            self.check_docs_pred()

        elif option == INSERT_CLIENT:
                text = ','.join(data[1:])
                id = getShaRepr(','.join(data[1:min(len(data),5)]))
                node = self.find_succ(id // 8192)
                clock_copy = self.clock.send_event()
                node._send_data(INSERT, f'documentos,{id},{text},|||,{clock_copy},|||', clock=clock_copy)

        elif option == GET_CLIENT:
                id = int(data[1])
                node = self.find_succ(id // 8192)
                clock_copy = self.clock.send_event()
                data_resp = node._send_data(GET, f'{id}', clock=clock_copy)

        elif option == REMOVE_CLIENT:
                id = int(data[1])
                node = self.find_succ(id // 8192)
                clock_copy = self.clock.send_event()
                node._send_data(REMOVE, f'documentos,{id}', clock=clock_copy)

        elif option == EDIT_CLIENT:
                id = int(data[1])
                text = ','.join(data[2:])
                node = self.find_succ(id // 8192)
                clock_copy = self.clock.send_event()
                node._send_data(EDIT, f'documentos,{id},{text},|||,{clock_copy},|||',clock=clock_copy)

        elif option == SEARCH_CLIENT:
                query = ','.join(data[1:])

                """if 'what design factors can be used to control lift-drag ratios' in query:
                    self.ref._send_data_global(GIVE_TIME)"""

                responses = []
                recv = threading.Thread(target=self.recv_query_responses, args=(responses,))
                recv.start()
                clock_copy = self.clock.send_event()
                self.ref._send_data_global(SEARCH, query, clock=clock_copy)
                recv.join()

                data_resp = '&&&'.join(responses).encode()

        
        if data_resp is not None and (option == GET_CLIENT or option == SEARCH_CLIENT or option == GET or option == OWNER):
            conn.sendall(data_resp)

        elif data_resp:
            response = f'{data_resp.id},{data_resp.ip}'.encode()
            conn.sendall(response)
        conn.close()

    # Start server method to handle incoming requests
    def start_server(self):

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: 

            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
            s.bind((self.ip, self.port)) 
            s.listen(10) 

            while True:
                conn, addr = s.accept()
                data = conn.recv(4096).decode()
                data = decode_response(data, split_char=',', char='¬')
                threading.Thread(target=self.data_receive, args=(conn, addr, data)).start()
      
    def notify(self, node: 'ChordNodeReference'):
        try:
            while self.loading:
                logger.debug('Notify Loading...')
                time.sleep(10)

            self.loading = True
            logger.debug('Notify Clear')

            super().notify(node)

            self.check_docs()
            if self.pred:
                clock_copy = self.clock.send_event()
                self.pred._send_data(CHECK_DOCKS, clock=clock_copy)

            self.loading = False
            logger.debug('Notify End')

        except:
            self.loading = False
            logger.debug('Notify Failed')
    
    def get_docs_between(self, tables, min, max):
        return self.controller.get_docs_between(tables, min, max)
    
    def check_docs(self):
        # toma sus documentos y las replicas de su predecesor
        my_docs = self.get_docs('documentos')
        pred_docs = self.get_docs('replica_pred')

        for doc in my_docs:
            # si el id NO esta entre su nuevo predecesor y el, o sea le pertenece a su predecesor
            if self.pred and not self._inbetween(doc[0] // 8192, self.pred.id, self.id):
                
                # le dice que lo inserte en sus documentos
                clock_copy1 = self.clock.send_event()
                self.pred._send_data(INSERT, f'documentos,{doc[0]},{doc[1]},|||,{doc[2]},|||', clock=clock_copy1) # Este paso asegura la replicación a ambos lados del vecino
                
                # lo elimina de sus documentos
                self.del_doc(doc[0], 'documentos')
                clock_copy2 = self.clock.send_event()
                self.succ._send_data(REMOVE, f'replica_pred,{doc[0]}', clock=clock_copy2)
            
            else:
                # esta entre los 2, asi que le pertenece al sucesor y le notifica que lo replique
                if self.pred:
                    clock_copy3 = self.clock.send_event()
                    self.pred._send_data(INSERT, f'replica_succ,{doc[0]},{doc[1]},|||,{doc[2]},|||', clock=clock_copy3)

        
        for doc in pred_docs:
            # si el id NO esta entre su nuevo predecesor y el, o sea le pertenece al antiguo predecesor
            if self.pred and not self._inbetween(doc[0] // 8192, self.pred.id, self.id):
                
                # lo elimina porque cambio su predecesor
                self.del_doc(doc[0], 'replica_pred')

            else:
                # si el id esta entre su nuevo predecesor y el, o sea le pertenece a el
                self.add_doc(id=doc[0], document=doc[1], clock=doc[2],table='documentos')

                # luego lo elimina de sus replicados
                self.del_doc(doc[0], 'replica_pred')

                # despues lo mandan a replicar
                clock_copy4 = self.clock.send_event()
                if self.pred:
                    self.pred._send_data(INSERT, f'replica_succ,{doc[0]},{doc[1]},|||,{doc[2]},|||', clock=clock_copy4)
                self.succ._send_data(INSERT, f'replica_pred,{doc[0]},{doc[1]},|||,{doc[2]},|||', clock=clock_copy4)

    # luego aqui entra el predecesor
    def check_docs_pred(self):
        
        # toma sus documentos y las replicas de su sucesor
        my_docs = self.get_docs('documentos')
        succ_docs = self.get_docs('replica_succ')

        for doc in my_docs:
           
            # los documentos que me pertenecen los replico a mi nuevo sucesor
            clock_copy1 = self.clock.send_event()
            self.succ._send_data(INSERT, f'replica_pred,{doc[0]},{doc[1]},|||,{doc[2]},|||',clock=clock_copy1)


        for doc in succ_docs:
            # si el id NO esta entre su nuevo sucesor y el, o sea le pertenece al antiguo sucesor
            if not self._inbetween(doc[0] // 8192, self.id, self.succ.id):

                # le dice que lo replique como documento del que ahora es el sucesor del nuevo sucesor
                clock_copy2 = self.clock.send_event()
                self.succ._send_data(INSERT, f'replica_succ,{doc[0]},{doc[1]},|||,{doc[2]},|||',clock=clock_copy2)
                
                # lo elimina porque cambio su sucesor
                self.del_doc(doc[0], 'replica_succ')
            
            else:
                # si el id esta entre su nuevo sucesor y el, o sea le pertenece al nuevo sucesor
                clock_copy3 = self.clock.send_event()
                self.succ._send_data(INSERT, f'documentos,{doc[0]},{doc[1]},|||,{doc[2]},|||', clock=clock_copy3)


