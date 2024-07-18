import socket, threading, time
import logging
PORT = '8005'
MCASTADDR = '224.0.0.1'
ID = str(socket.gethostbyname(socket.gethostname()))

OK = 2
ELECTION = 1
WINNER = 3

def broadcast_call(message: str, port: str):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Cambiamos el TTL y configuramos la dirección de broadcast
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    s.sendto(message.encode(), ('<broadcast>', port))
    s.close()


class BullyBroadcastElector:

    def __init__(self):
        self.id = str(socket.gethostbyname(socket.gethostname()))
        self.port = int(PORT)
        self.Leader = None
        self.InElection = False
        self.ImTheLeader = True

    def bully(self, id: str, otherId: str):
        return int(id.split('.')[-1]) > int(otherId.split('.')[-1])

    def election_call(self):
        t = threading.Thread(target=broadcast_call,args=(f'{ELECTION}', self.port))
        t.start() 
        print("Election Started")

    def winner_call(self):
        t = threading.Thread(target=broadcast_call,args=(f'{WINNER}', self.port))
        t.start() 

    def loop(self):
        t = threading.Thread(target=self.server_thread)
        t.start()

        counter = 0
        while True:
            #SIempre hay lider segun todos los nodos
            # logging.debug(f"=========================================")
            # logging.debug(f"El lider segun {self.id} => {self.Leader}")
            # logging.debug(f"=========================================")
            
            if not self.Leader and not self.InElection:
                self.election_call()
                self.InElection = True

            elif self.InElection:
                counter += 1
                if counter == 3:
                    if not self.Leader and self.ImTheLeader:
                        self.ImTheLeader = True
                        self.Leader = self.id
                        self.InElection = False
                        self.winner_call()
                    counter = 0
                    self.InElection = False

            else:
                print(f'Leader: {self.Leader}')

            # print(f"{counter} waiting")
            time.sleep(1)

    def server_thread(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        s.bind(('', self.port))

        while True:
            try:
                msg, sender = s.recvfrom(1024)
                if not msg:
                    continue  # Ignorar mensajes vacíos

                newId = sender[0]
                msg = msg.decode("utf-8")

                if msg.isdigit():
                    msg = int(msg)
                    if msg == ELECTION and not self.InElection:
                        print(f"Election message received from: {newId}")

                        if not self.InElection:
                            self.InElection = True
                            # self.Leader = None
                            self.election_call()

                        if self.bully(self.id, newId):
                            s_send = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                            s_send.sendto(f'{OK}'.encode(), (newId, self.port))

                    elif msg == OK:
                        print(f"OK message received from: {newId}")
                        if self.Leader and self.bully(newId, self.Leader):
                            self.Leader = newId
                        self.ImTheLeader = False

                    elif msg == WINNER:
                        print(f"Winner message received from: {newId}")
                        if not self.bully(self.id, newId) and (not self.Leader or self.bully(newId, self.Leader)):
                            self.Leader = newId
                            if(self.Leader != self.id):
                                self.ImTheLeader = False
                            self.InElection = False

            except Exception as e:
                print(f"Error in server_thread: {e}")