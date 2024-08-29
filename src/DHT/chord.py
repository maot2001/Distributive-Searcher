import socket
import threading
import time
import hashlib
import logging

from DHT.election import BullyBroadcastElector
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

# Function to hash a string using SHA-1 and return its integer representation
def getShaRepr(data: str, max_value: int = 262144):
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

# Class to reference a Chord node
class ChordNodeReference:
    def __init__(self, ip: str, port: int = 8001):
        self.id = getShaRepr(ip)
        self.ip = ip
        self.port = PORT

    # Internal method to send data to the referenced node
    def _send_data(self, op: int, data: str = None) -> bytes:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                if op == CHECK_NODE: s.settimeout(2)
                s.connect((self.ip, self.port))
                s.sendall(f'{op},{data}'.encode('utf-8'))
                return s.recv(1024)
        except Exception as e:
            return b''
        
    # Internal method to send data to all nodes
    def _send_data_global(self, op: int, data: str = None) -> list:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            s.sendto(f'{op},{data}'.encode(), (str(socket.INADDR_BROADCAST), PORT))
            if op != SEARCH:
                response = s.recv(1024).decode().split(',')
            s.close()
            if op != SEARCH:
                return response
        except Exception as e:
            return b''
               
    # Method to find a chord network node to conect
    def join(self, ip) -> list:
        response = self._send_data_global(JOIN, ip)
        return response

    # Method to find the successor of a given id
    def find_successor(self, id: int) -> 'ChordNodeReference':
        response = self._send_data(FIND_SUCCESSOR, str(id)).decode().split(',')
        return ChordNodeReference(response[1], self.port)

    # Method to find the predecessor of a given id
    def find_predecessor(self, id: int) -> 'ChordNodeReference':
        response = self._send_data(FIND_PREDECESSOR, str(id)).decode().split(',')
        return ChordNodeReference(response[1], self.port)

    # Property to get the successor of the current node
    @property
    def succ(self) -> 'ChordNodeReference':
        response = self._send_data(GET_SUCCESSOR).decode().split(',')
        return ChordNodeReference(response[1], self.port)

    # Property to get the predecessor of the current node
    @property
    def pred(self) -> 'ChordNodeReference':
        response = self._send_data(GET_PREDECESSOR).decode().split(',')
        return ChordNodeReference(response[1], self.port)

    # Method to notify the current node about another node
    def notify(self, node: 'ChordNodeReference'):
        self._send_data(NOTIFY, f'{node.id},{node.ip}')

    def notify_pred(self, node: 'ChordNodeReference'):
        self._send_data(NOTIFY_PRED, f'{node.id},{node.ip}')

    # Method to check if the predecessor is alive
    def check_node(self):
        return self._send_data(CHECK_NODE)

    # Method to find the closest preceding finger of a given id
    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        response = self._send_data(CLOSEST_PRECEDING_FINGER, str(id)).decode().split(',')
        return ChordNodeReference(response[1], self.port)

    # Method to store a key-value pair in the current node
    def store_key(self, key: str, value: str):
        self._send_data(STORE_KEY, f'{key},{value}')

    # Method to retrieve a value for a given key from the current node
    def retrieve_key(self, key: str) -> str:
        response = self._send_data(RETRIEVE_KEY, key).decode()
        return response

    def __str__(self) -> str:
        return f'{self.id},{self.ip},{self.port}'

    def __repr__(self) -> str:
        return str(self)


# Class representing a Chord node
class ChordNode:
    def __init__(self, ip: str, port: int = 8001, m: int = 160, election: bool = False):
        self.id = getShaRepr(ip)
        self.ip = ip
        self.port = PORT
        self.ref = ChordNodeReference(self.ip, self.port)
        self.succ = self.ref  # Initial successor is itself
        self.pred = None  # Initially no predecessor
        self.m = m  # Number of bits in the hash/key space
        self.finger = [self.ref] * self.m  # Finger table
        self.next = 0  # Finger table index to fix next
        self.data = {}  # Dictionary to store key-value pairs
        self.election = election
        #### Bully
        if self.election:
            self.e = BullyBroadcastElector()
            threading.Thread(target=self.e.server_thread, daemon=True).start()
            threading.Thread(target=self.e.loop, daemon=True).start()
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
        return node.succ  # Return successor of that node

    # Method to find the predecessor of a given id
    def find_pred(self, id: int, direction=True) -> 'ChordNodeReference':
        node = self
        if direction:
            while True:
                if isinstance(node, ChordNodeReference) and node.succ == b'':
                    return self.find_pred(id, False)
                if not self._inbetween(id, node.id, node.succ.id):
                    node = node.succ
                else: break
        else:
            while True:
                if isinstance(node, ChordNodeReference) and node.pred == b'':
                    return self.find_pred(id, True)
                if not self._inbetween(id, node.pred.id, node.id):
                    node = node.pred
                else: break
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
            self.succ = node.find_successor(self.id)
            self.succ.notify(self.ref)
        else:
            self.succ = self.ref
            self.pred = None
      
    # Method to join a Chord network without 'node' reference as an entry point      
    def join_CN(self):
        msg = self.ref.join(self.ref)
        return self.join(ChordNodeReference(msg[2], PORT))
    
    # Stabilize method to periodically verify and update the successor and predecessor
    def stabilize(self):
        while True:
            if self.succ.id != self.id:
                if self.succ.check_node() != b'':
                    x = self.succ.pred
                    if x.id != self.id:
                        logger.debug(x)
                        if x and self._inbetween(x.id, self.id, self.succ.id):
                            self.succ = x
                        self.succ.notify(self.ref)

            logger.debug(f"successor : {self.succ} predecessor {self.pred}")
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
                self.succ.notify(self.ref)
        elif self._inbetween(node.id, self.pred.id, self.id):
            # self.pred.notify_pred(node)
            self.pred = node

    def notify_pred(self, node: 'ChordNodeReference'):
        logger.debug(f'in notify_pred, my id: {self.id} my succ: {node.id}')
        self.succ = node
        self.succ.notify(self.ref)

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
        while True:
            if self.pred and self.pred.check_node() == b'':
                logger.debug('\n\n\n ALARMA!!! PREDECESOR PERDIDO!!! \n\n\n')
                if self.election:
                    self.e.Leader = None
                    self.e.ImTheLeader = True
                    threading.Thread(target=self.e.loop, daemon=True).start()
                pred = self.find_pred(self.pred.id)
                self.pred = None
                pred.notify_pred(self.ref)
            time.sleep(10)

    # Store key method to store a key-value pair and replicate to the successor
    def store_key(self, key: str, value: str):
        key_hash = getShaRepr(key)
        node = self.find_succ(key_hash)
        node.store_key(key, value)
        self.data[key] = value  # Store in the current node
        self.succ.store_key(key, value)  # Replicate to the successor

    # Retrieve key method to get a value for a given key
    def retrieve_key(self, key: str) -> str:
        key_hash = getShaRepr(key)
        node = self.find_succ(key_hash)
        return node.retrieve_key(key)
    
    # Reciev boradcast message 
    def _reciev_broadcast(self):
        
        while True:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.bind(('', int(PORT)))
            msg, addr = s.recvfrom(1024)
            
            msg = msg.decode().split(',')
            option = int(msg[0])

            if option == JOIN:
                if msg[2] != self.ip:
                    try:
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                            s.connect((msg[2], self.port))
                            s.sendall(f'{JOIN},{self.ref}'.encode('utf-8'))
                    except Exception as e:
                        logger.debug(f"Error in JOIN: {e}")

            elif option == SEARCH:
                try:
                    query = ','.join(msg[1:])
                    docs = '$$$'.join(self.search(query))
                    response = f"{SEARCH_SLAVE},{docs}".encode()
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect((addr[0], 8002))
                        s.sendall(response)
                except Exception as e:
                    logger.debug(f'Error in SEARCH: {e}')

            s.close()
