import socket
import threading
import time
import hashlib
import logging

from DHT.election import BullyBroadcastElector
from DHT.clocks import VectorClock
# Configurar el nivel de log
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')

logger = logging.getLogger(__name__)

PORT = 8001

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
SEARCH_SLAVE = 30
GIVE_TIME = 31
SET_PRE_PRED = 32
# Function to hash a string using SHA-1 and return its integer representation
def getShaRepr(data: str, max_value: int = 2097152):
    # Genera el hash SHA-1 y obtén su representación en hexadecimal
    hash_hex = hashlib.sha1(data.encode()).hexdigest()
    
    # Convierte el hash hexadecimal a un entero
    hash_int = int(hash_hex, 16)
    
    # Define un arreglo o lista con los valores del 0 al 16
    values = list(range(max_value + 1))
    
    # Usa el hash como índice para seleccionar un valor del arreglo
    # Asegúrate de que el índice esté dentro del rango válido
    index = hash_int % len(values)
    
    # Devuelve el valor seleccionado
    return values[index]

def decode_response(S, char=None, split_char = None):
    if S == '': #Un error al acceder a un nodo devuelve la misma cadena vacía
        # logger.debug(f"Que raro")
        return b''
    if not char:
        if split_char:
            S = S.split(split_char)
            return [x for x in S if x]
        return S
    pos = -1
    for i, c in enumerate(S):
        if c == char:
            pos = i
            break
    if pos == -1:
        if split_char:
            S = S.split(split_char)
            return [x for x in S if x]
        return S
    if split_char:
        prefS = S[:pos].split(split_char)
        # logger.debug(f"Hola prefS = {prefS}")
        return [x for x in prefS if x] + [S[pos+1:]]
    return S[:pos] + S[pos+1:]

# Class to reference a Chord node
class ChordNodeReference:
    def __init__(self, ip: str, port: int = 8001):
        self.id = getShaRepr(ip, 256)
        self.ip = ip
        self.port = PORT

    # Internal method to send data to the referenced node
    def _send_data(self, op: int, data: str = None, clock = b'', size = 2048) -> bytes:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                if op == CHECK_NODE: s.settimeout(2)
                s.connect((self.ip, self.port))
                if clock != b'':
                    s.sendall(f'{op},{data},¬{clock}'.encode('utf-8'))
                else:
                    s.sendall(f'{op},{data}'.encode('utf-8'))
                return s.recv(size)
        except Exception as e:
            return b''
        
    # Internal method to send data to all nodes
    def _send_data_global(self, op: int, data: str = None, clock = b'') -> list:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            if clock != b'':
                s.sendto(f'{op},{data},¬{clock}'.encode(), (str(socket.INADDR_BROADCAST), PORT))
            else:
                s.sendto(f'{op},{data}'.encode(), (str(socket.INADDR_BROADCAST), PORT))
            if op != SEARCH and op != GIVE_TIME:
                response = s.recv(2048).decode()
                response = decode_response(response, split_char=',', char="¬")
            s.close()
            if op != SEARCH and op != GIVE_TIME:
                return response
        except Exception as e:
            return b''
               
    # Method to find a chord network node to conect
    def join(self, ref, clock = b'') -> list:
        response = self._send_data_global(JOIN, ref, clock=clock)
        response = decode_response(response, split_char=',', char='¬')
        return response

    # Method to find the successor of a given id
    def find_successor(self, id: int, clock = b'') -> 'ChordNodeReference':
        response = self._send_data(FIND_SUCCESSOR, str(id), clock=clock).decode()
        response = decode_response(response, split_char=',', char='¬')
        if response == b'': return response
        return ChordNodeReference(response[1], self.port), response[-1]

    # Method to find the predecessor of a given id
    def find_predecessor(self, id: int, clock = b'') -> 'ChordNodeReference':
        response = self._send_data(FIND_PREDECESSOR, str(id), clock=clock).decode()
        response = decode_response(response, split_char=',', char='¬')
        if response == b'': return response
        return ChordNodeReference(response[1], self.port), response[-1]

    # Property to get the successor of the current node
    def succ(self, clock = b'') -> 'ChordNodeReference':
        response = self._send_data(GET_SUCCESSOR, clock=clock).decode()
        response = decode_response(response, split_char=',', char='¬')
        if response == b'': return response
        return ChordNodeReference(response[1], self.port), response[-1]

    # Property to get the predecessor of the current node
    def pred(self, clock = b'') -> 'ChordNodeReference':
        response = self._send_data(GET_PREDECESSOR, clock=clock).decode()
        response = decode_response(response, split_char=',', char='¬')
        if response == b'': return response
        return ChordNodeReference(response[1], self.port), response[-1]

    # Method to notify the current node about another node
    def notify(self, node: 'ChordNodeReference', node_pred = '' ,clock = b''):
        if node_pred == '':
            self._send_data(NOTIFY, f'{node.id},{node.ip}', clock= clock)
        else:
            self._send_data(NOTIFY, f'{node.id},{node.ip},{node_pred}', clock= clock)

    def notify_pred(self, node: 'ChordNodeReference', clock = b''):
        self._send_data(NOTIFY_PRED, f'{node.id},{node.ip}', clock=clock)

    # Method to check if the predecessor is alive
    def check_node(self, clock = b''):
        response = self._send_data(CHECK_NODE,clock=clock).decode()
        # logger.debug(f"CCN response = {response}")
        response = decode_response(response, split_char=',', char='¬')
        return response

    # Method to find the closest preceding finger of a given id
    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        response = self._send_data(CLOSEST_PRECEDING_FINGER, str(id)).decode().split(',')
        return ChordNodeReference(response[1], self.port)

    # Method to store a key-value pair in the current node
    def store_key(self, key: str, value: str, clock=b''):
        self._send_data(STORE_KEY, f'{key},{value}', clock=clock)

    # Method to retrieve a value for a given key from the current node
    def retrieve_key(self, key: str, clock=b'') -> str:
        response = self._send_data(RETRIEVE_KEY, key, clock=clock).decode()
        return response

    def __str__(self) -> str:
        return f'{self.id},{self.ip},{self.port}'

    def __repr__(self) -> str:
        return str(self)


# Class representing a Chord node
class ChordNode:
    def __init__(self, ip: str, port: int = 8001, m: int = 160, election: bool = False):
        self.id = getShaRepr(ip, 256)
        self.ip = ip
        self.port = PORT
        self.ref = ChordNodeReference(self.ip, self.port)
        self.succ = self.ref  # Initial successor is itself
        self.pred = None  # Initially no predecessor
        self.pre_pred = None
        self.pre_pred_aux = None
        self.m = m  # Number of bits in the hash/key space
        self.clock = VectorClock(256, self.id)
        self.finger = [self.ref] * self.m  # Finger table
        self.next = 0  # Finger table index to fix next
        self.data = {}  # Dictionary to store key-value pairs
        self.election = election
        self.time = 0
        self.included = False

        #### Bully
        if self.election:
            self.e = BullyBroadcastElector()
            threading.Thread(target=self.e.server_thread, daemon=True).start()
            threading.Thread(target=self.e.loop).start()
        ####

        # Start background threads for stabilization, fixing fingers, and checking predecessor
        threading.Thread(target=self.stabilize, daemon=True).start()  # Start stabilize thread
        # threading.Thread(target=self.fix_fingers, daemon=True).start()  # Start fix fingers thread
        threading.Thread(target=self.check_predecessor, daemon=True).start()  # Start check predecessor thread
        # threading.Thread(target=self.start_server, daemon=True).start()  # Start server thread
        threading.Thread(target=self._reciev_broadcast, daemon=True).start() ## Reciev broadcast message

    # Helper method to check if a value is in the range (start, end]
    def _inbetween(self, k: int, start: int, end: int) -> bool:
        if start < end:
            return start < k <= end
        else:  # The interval wraps around 0
            return start < k or k <= end

    # Method to find the successor of a given id
    def find_succ(self, id: int) -> 'ChordNodeReference':
        node = self.find_pred(id)  # Find predecessor of id
        if isinstance(node, ChordNodeReference):
            clock_copy = self.clock.send_event()
            x = node.succ(clock= clock_copy)  # Return successor of that node
            if x != b'':
                self.clock.update(x[-1])
                return x[0] 
            return x
        else:
            return node.succ

    # Method to find the predecessor of a given id
    def find_pred(self, id: int, direction=True, isCP = False) -> 'ChordNodeReference':
        node = self
        aux = node

        if direction:
            while True:
                if isinstance(node, ChordNodeReference):
                    clock_copy1 = self.clock.send_event()
                    x = node.succ(clock= clock_copy1)
                    if x == b'':
                        if not isCP:
                            return self.find_pred(id, False)
                        else:
                            node = aux
                            break
                    #logger.debug(f'\n ELSE!!! DIRECTION: {direction} node.id {node.id} and x.id{x[0].id} inbetween {self._inbetween(id, node.id, x[0].id)} !!! \n')
                    if not self._inbetween(id, node.id, x[0].id):
                        aux = node
                        node = x[0]
                    else: break
                    continue
                if not isinstance(node, ChordNodeReference) and not self._inbetween(id, node.id, node.succ.id): # nodo sigue siendo self
                    node = node.succ
                else: 
                    #logger.debug(f'\n ELSE!!! DIRECTION: {direction} node {node}!!! \n')
                    break
        else:
            while True:
                if isinstance(node, ChordNodeReference):
                    clock_copy2 = self.clock.send_event()
                    y = node.pred(clock= clock_copy2)
                    if  y == b'':
                        return self.find_pred(id, True)
                    if not self._inbetween(id, y[0].id, node.id):
                        node = y[0]
                    else: break
                    continue
                if not isinstance(node, ChordNodeReference) and not self._inbetween(id, node.pred.id, node.id):  # nodo sigue siendo self
                    node = node.pred
                else:
                    #logger.debug(f'\n ELSE!!! DIRECTION: {direction} node {node}!!! \n') 
                    break
        #logger.debug(f'\n ALARMA!!! nodo pred {node}!!! \n')
        return node

    # Method to find the closest preceding finger of a given id
    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        for i in range(self.m - 1, -1, -1):
            if self.finger[i] and self._inbetween(self.finger[i].id, self.id, id):
                return self.finger[i].closest_preceding_finger(id)
        
        return self.ref

    # Method to join a Chord network using 'node' as an entry point
    def join(self, node: 'ChordNodeReference'):
        if node:
            self.pred = None
            clock_copy1 = self.clock.send_event()
            x = node.find_successor(self.id, clock=clock_copy1) # ? Si se cae hacer un nuevo broadcast
            if x != b'': # todo ver como se hace para volver a entrar en la red una vez que se caiga tu vecino
                self.succ = x[0] 
                clock_sent = x[-1]
                self.clock.update(clock_sent)
                clock_copy2 = self.clock.send_event()
                self.succ.notify(self.ref, clock=clock_copy2)
        else:
            self.succ = self.ref
            self.pred = None
      
    # Method to join a Chord network without 'node' reference as an entry point      
    def join_CN(self):
        clock_copy1 = self.clock.send_event()
        msg = self.ref.join(self.ref, clock=clock_copy1)
        self.clock.update(msg[-1]) # ! Me parece que nunca llegara esta respuesta ¿afecta?
        return self.join(ChordNodeReference(msg[2], PORT))
    
    # Stabilize method to periodically verify and update the successor and predecessor
    def stabilize(self):
        while True:
            if self.succ.id != self.id:
                clock_copy1 = self.clock.send_event()
                succ_answer = self.succ.check_node(clock=clock_copy1)
                if succ_answer != b'':
                    # logger.debug(f"HolaST succ_answer = {succ_answer}")
                    self.clock.update(succ_answer[-1])
                    clock_copy2 = self.clock.send_event()
                    x = self.succ.pred(clock= clock_copy2)
                    if x != b'':
                        if x[0].id != self.id:
                            clock_copy3 = x[-1]
                            self.clock.update(clock_copy3)
                            # logger.debug(x)
                            if x[0] and self._inbetween(x[0].id, self.id, self.succ.id):
                                self.succ = x[0]
                            clock_copy4 = self.clock.send_event()
                            self.succ.notify(self.ref, clock= clock_copy4)

                        if self.pred and self.pred.id != self.succ.id:
                            self.pre_pred_aux = self.pred
                            clock_copy5 = self.clock.send_event()
                            self.succ._send_data(SET_PRE_PRED,self.pred,clock_copy5)
                        #elif self.pred and (not self.pre_pred_aux or self.pre_pred_aux.id !=  self.pred.id):
                        #    self.pre_pred_aux = self.pred
                        #    clock_copy5 = self.clock.send_event()
                        #    self.succ._send_data(SET_PRE_PRED,self.pred,clock_copy5)

                        
                        

            logger.debug(f"my id: {self.id} successor: {self.succ} predecessor: {self.pred} prepredecessor: {self.pre_pred} leader: {self.e.Leader}")
            time.sleep(10)

    # Notify method to inform the node about another node
    def notify(self, node: 'ChordNodeReference'):
        logger.debug(f'in notify, my id: {self.id} my pred: {node.id}')
        if node.id == self.id:
            pass
        elif not self.pred:
            self.pred = node
            if self.id == self.succ.id:
                self.succ = node
                clock_copy1 = self.clock.send_event()
                # logger.debug(f'sending NOTIFY to {self.succ} for create ring')
                self.succ.notify(self.ref, clock= clock_copy1)
        elif self._inbetween(node.id, self.pred.id, self.id):
            self.pred = node
            #if self.pred.id != self.succ.id:
            #    clock_copy2 = self.clock.send_event()
            #    self.succ._send_data(SET_PRE_PRED,self.pred,clock_copy2)

    def notify_pred(self, node: 'ChordNodeReference'):
        # logger.debug(f'in notify_pred, my id: {self.id} my succ: {node.id}')
        self.succ = node
        # logger.debug(f'sending NOTIFY to {self.succ} for notify_pred')
        clock_copy1 = self.clock.send_event()
        self.succ.notify(self.ref, clock= clock_copy1)

    # Fix fingers method to periodically update the finger table
    def fix_fingers(self):
        while True:
            to_write = ''
            for i in range(self.m):
                # Calcular el próximo índice de dedo
                next_index = (self.id + 2**i) % 2**self.m
                if self.succ.id == self.id:
                    self.finger[i] = self.ref
                else:
                    if self._inbetween(next_index, self.id, self.succ.id):
                        self.finger[i] = self.succ
                    else:
                        node = self.succ.closest_preceding_finger(next_index)
                        if node.id != next_index:
                            node = node.succ
                        self.finger[i] = node
                
                if i == 0 or self.finger[i-1].id != self.finger[i].id or i > 154:
                    to_write += f'>> ({i}, {next_index}): {self.finger[i].id}\n'
            logger.debug(f'fix_fingers {self.id}: {self.succ} and {self.pred}')
            logger.debug(f'{self.id}:\n{to_write}')
            time.sleep(10)

    # Check predecessor method to periodically verify if the predecessor is alive
    def check_predecessor(self):
        
        aux_node = None
        while True:
            if self.pred:
                clock_copy1 = self.clock.send_event()
                x = self.pred.check_node(clock_copy1)
                if x == b'':
                    aux = True
                    logger.debug('\n\n\n ALARMA!!! PREDECESOR PERDIDO!!! \n\n\n')
                    if self.election:
                        self.e.Leader = None
                        self.e.ImTheLeader = True
                        threading.Thread(target=self.e.loop, daemon=True).start()
                    
                    if self.pre_pred: # La red tiene al menos tres nodos 
                        aux_node = self.pred.id
                        pred = self.pre_pred
                        self.pre_pred = None
                        self.pred = None
                        aux = False
                        clock_copy2 = self.clock.send_event()
                        pred.notify_pred(self.ref, clock=clock_copy2)
                        
                        time.sleep(2)
                    if not self.pred: # El predecesor de mi predecesor desaparecio 
                        pred = self.find_pred(aux_node, isCP=True) # puede darse el caso que no encuentres nada
                        clock_copy3 = self.clock.send_event()
                        if isinstance(pred, ChordNodeReference):
                            pred.notify_pred(self.ref, clock=clock_copy3)
                        else:
                            pred.notify_pred(self.ref)
                        continue

                    if aux:
                        
                        pred = self.find_pred(self.pred.id, isCP=True) # puede darse el caso que no encuentres nada
                        self.pred = None
                        clock_copy3 = self.clock.send_event()
                        if isinstance(pred, ChordNodeReference):
                            pred.notify_pred(self.ref, clock=clock_copy3)
                        else:
                            pred.notify_pred(self.ref)
                        continue
                    
                else:
                    #logger.debug(f"HolaCP self.pred.cn = {x}")
                    clock_sent = x[-1]
                    self.clock.update(clock_sent)
                    if self.pre_pred and self.pred.id == self.succ.id:
                        self.pre_pred = None
                time.sleep(10)
            

    # Store key method to store a key-value pair and replicate to the successor
    def store_key(self, key: str, value: str):
        key_hash = getShaRepr(key)
        node = self.find_succ(key_hash)
        clock_copy2 = self.clock.send_event()
        node.store_key(key, value,clock=clock_copy2)
        self.data[key] = value  # Store in the current node
        clock_copy3 = self.clock.send_event() #? Necesario no basta con usar el mismo clock_copy2?
        self.succ.store_key(key, value, clock_copy3)  # Replicate to the successor

    # Retrieve key method to get a value for a given key
    def retrieve_key(self, key: str) -> str:
        key_hash = getShaRepr(key)
        node = self.find_succ(key_hash)
        clock_copy1 = self.clock.send_event()
        response = node.retrieve_key(key,clock=clock_copy1)
        #self.clock.update()
        return response
    
    # Reciev boradcast message 
    def _reciev_broadcast(self):
        
        while True:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.bind(('', int(PORT)))
            msg, addr = s.recvfrom(1024)
            
            msg = msg.decode()
            msg = decode_response(msg, split_char=',', char='¬')
            option = int(msg[0])

            if option == JOIN:
                if msg[2] != self.ip:
                    clock_sent = msg[-1]
                    self.clock.update(clock_sent)
                    try:
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                            clock_copy1 = self.clock.send_event()
                            s.connect((msg[2], self.port))
                            s.sendall(f'{JOIN},{self.ref},¬{clock_copy1}'.encode('utf-8')) #*Reloj
                    except Exception as e:
                        logger.debug(f"Error in JOIN: {e}")
                    if not self.pred and self.succ.id == self.id:
                        time.sleep(2)
                        if not self.pred:
                            self.join_CN()

            elif option == SEARCH:
                try:
                    clock_sent = msg[-1]
                    self.clock.update(clock_sent)
                    logger.debug(f'in SEARCH the msg is: {msg}')
                    query = ','.join(msg[1:-1])
                    logger.debug(f'in SEARCH the query is: {query}')

                    now = time.time()
                    docs = '$$$'.join(self.search(query))
                    now = time.time() - now
                    self.time = max(self.time, now)
                    logger.debug(f'in SEARCH time: {now}')
                    if now > self.waiter: logger.debug(f'\n\n\n\n\nTIME EXCEDED\n\n\n\n\n')

                    clock_copy2 = self.clock.send_event()
                    response = f"{SEARCH_SLAVE},{docs},¬{clock_copy2}".encode() #*Reloj
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect((addr[0], 8002))
                        s.sendall(response)
                except Exception as e:
                    logger.debug(f'Error in SEARCH: {e}')

            elif option == GIVE_TIME:
                logger.debug(f'\n\n\nMy worst time is {self.time}\n\n\n')
                self.time = 0

            s.close()
