import threading
import socket
from chord import ChordNode, ChordNodeReference

# Operation codes
FIND_SUCCESSOR = 1
FIND_PREDECESSOR = 2
GET_SUCCESSOR = 3
GET_PREDECESSOR = 4
NOTIFY = 5
CHECK_PREDECESSOR = 6
CLOSEST_PRECEDING_FINGER = 7
STORE_KEY = 8
RETRIEVE_KEY = 9
SEARCH = 10
JOIN = 11

class Node(ChordNode):    
    def __init__(self, ip: str, port: int = 8001, m: int = 160):
        super().__init__(ip, port, m)
        threading.Thread(target=self.start_server, daemon=True).start()  # Start server thread
    
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
                
                print(f'new connection from {addr}')

                data = conn.recv(1024).decode().split(',') # Divide el string del mensaje por las ","

                data_resp = None 

                option = int(data[0])

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
                    id = int(data[1])
                    ip = data[2]
                    self.notify(ChordNodeReference(ip, self.port))

                elif option == CHECK_PREDECESSOR:
                    pass

                elif option == CLOSEST_PRECEDING_FINGER:
                    id = int(data[1])
                    data_resp = self.closest_preceding_finger(id)

                elif option == STORE_KEY:
                    key, value = data[1], data[2]
                    self.data[key] = value

                elif option == RETRIEVE_KEY:
                    key = data[1]
                    data_resp = self.data.get(key, '')

                elif option == JOIN:
                    data_resp = self.ref

                

                if data_resp:
                    response = f'{data_resp.id},{data_resp.ip}'.encode()
                    conn.sendall(response)
                conn.close()