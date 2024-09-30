import socket, threading, time
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')

logger = logging.getLogger(__name__)

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
        self.InElectionSwap = False

    def bully(self, id: str, otherId: str):
        return int(id.split('.')[-1]) > int(otherId.split('.')[-1])

    def election_call(self):
        t = threading.Thread(target=broadcast_call,args=(f'{ELECTION}', self.port))
        t.start() 

    def winner_call(self):
        t = threading.Thread(target=broadcast_call,args=(f'{WINNER}', self.port))
        t.start() 

    def loop(self):
        counter = 0
        while True:
            counter += 1
            if not self.Leader and not self.InElection:
                self.InElection = True
                self.InElectionSwap = False
                #logger.debug(f"Election message sending")
                self.election_call()

            if self.InElection:
                #logger.debug(f"In Election counter {counter}")
                if counter == 3:
                    if not self.Leader and self.ImTheLeader:
                        self.Leader = self.id
                        self.winner_call()
                    self.InElection = False
                    counter = 0
                    break

            if counter == 3:
                break
            
            time.sleep(1)
        #logger.debug(f"Close Loop")
        

    def data_receive(self, newId, msg):
        msg = int(msg)
        if msg == ELECTION and newId != self.id:
            #logger.debug(f"Election message received from: {newId}")

            if not self.InElection and not self.InElectionSwap:
                #logger.debug(f"Election message passed from: {newId}")
                self.InElectionSwap = True
                self.Leader = None
                self.ImTheLeader = True
                threading.Thread(target=self.loop).start()

            if self.bully(self.id, newId):
                #logger.debug(f"OK message sending to: {newId}")
                s_send = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s_send.sendto(f'{OK}'.encode(), (newId, self.port))

        elif msg == OK:
            #logger.debug(f"OK message received from: {newId}")
            self.ImTheLeader = False

        elif msg == WINNER:
            #logger.debug(f"Winner message received from: {newId}")
            if not self.bully(self.id, newId) and (not self.Leader or self.bully(newId, self.Leader)):
                self.Leader = newId
                if self.Leader != self.id:
                    self.ImTheLeader = False
                self.InElection = False
            #else:
                #logger.debug(f'Reject Winner message from {newId}, my leader is {self.Leader}')

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

                threading.Thread(target=self.data_receive, args=(newId, msg)).start()

            except Exception as e:
                logger.debug(f"Error in server_thread: {e}")