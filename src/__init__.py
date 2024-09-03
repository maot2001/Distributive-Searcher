import os
import shutil
import socket
import sys
import json
from DHT.node import Node, getShaRepr

if __name__ == "__main__":

    if len(sys.argv) < 2:
        for f in os.listdir('src/data'):
            shutil.rmtree(os.path.join('src/data', f))
    
    host_name = socket.gethostname() 
    ip = socket.gethostbyname(host_name)
    node = Node(ip, election=True)

    if len(sys.argv) >= 2:
        node.join_CN()
    else:
        with open('src/local_db/docs.json', 'r', encoding='utf-8') as f:
            dataset = json.load(f)

        change = {}
        for doc in dataset:
            if int(doc) > 5: break
            text = dataset[doc].split(',')
            clock_ini = [0] * node.m #! provicional esto debe estar dado por el limite de la red (el cual tengo en el parámetro m)
            id = getShaRepr(','.join(text[0:min(len(text),4)]))
            node.add_doc(id=id, document=dataset[doc], clock=str(clock_ini),table='documentos')
            if id in change: raise Exception
            change[id] = doc

        change_json = json.dumps(change, indent=4, ensure_ascii=False)
        with open('src/local_db/change.json', 'w', encoding='utf-8') as file:
            file.write(change_json)

        print('done!')

    while True:
        pass