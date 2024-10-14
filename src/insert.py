import threading
import time
import socket
import json
from Presentation import ClientNode, getShaRepr, ChordNodeReference

class InsertNode(ClientNode):
    def __init__(self, ip: str, port: int = 8001):
        self.id = getShaRepr(ip, 256)
        self.ip = ip
        self.port = port
        self.ref = ChordNodeReference(self.ip, self.port)
        self.connect = None

        threading.Thread(target=self.start_server, daemon=True).start()

        self.join_CN(self.ref)
        
        while self.connect is None:
            time.sleep(2)   

        self.insert_all()     


    def start_server(self):

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: 

            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
            s.bind((self.ip, self.port)) 
            s.listen(10) 

            while True:
                conn, addr = s.accept()
                data = conn.recv(1024).decode().split(',')

                threading.Thread(target=self.data_receive, args=(conn, addr, data)).start()

    def insert_all(self):
        with open('src/local_db/docs.json', 'r', encoding='utf-8') as f:
            dataset = json.load(f)

        change = {}
        for doc in dataset:
            #if int(doc) > 19: break
            if int(doc) % 100 == 0: print(doc)
            self.insert(dataset[doc])
            text = dataset[doc].split(',')
            id = getShaRepr(','.join(text[0:min(len(text),4)]))
            change[id] = doc

        change_json = json.dumps(change, indent=4, ensure_ascii=False)
        with open('src/local_db/change.json', 'w', encoding='utf-8') as file:
            file.write(change_json)

        print('done!')


host_name = socket.gethostname() 
ip = socket.gethostbyname(host_name)
client = InsertNode(ip)