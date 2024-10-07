import threading
import time
import socket
import json
from Presentation import ClientNode, getShaRepr, ChordNodeReference

class MetricNode(ClientNode):
    def __init__(self, ip: str, port: int = 8001):
        self.id = getShaRepr(ip, 256)
        self.ip = ip
        self.port = port
        self.ref = ChordNodeReference(self.ip, self.port)
        self.connect = None

        with open('src/local_db/change.json', 'r', encoding='utf-8') as f:
            self.changes = json.load(f)

        with open('src/local_db/query.json', 'r', encoding='utf-8') as f:
            self.query = json.load(f)

        threading.Thread(target=self.start_server, daemon=True).start()

        print('thread')

        self.join_CN(self.ref)
        print('join')
        
        while self.connect is None:
            time.sleep(2)        
        print('metrics')

        with open('src/local_db/metrics.json', 'r', encoding='utf-8') as f:
            results = json.load(f)
        
        val = self.metrics()
        print(val)
        """results['precision'].append(val[0])
        results['recall'].append(val[1])
        results['f_measure'].append(val[2])
        results['r_precision'].append(val[3])
        results['failure_ratio'].append(val[4])
            
        results_json = json.dumps(results, indent=4, ensure_ascii=False)
        with open('src/local_db/metrics.json', 'w', encoding='utf-8') as file:
            file.write(results_json)"""

    def start_server(self):

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: 

            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
            s.bind((self.ip, self.port)) 
            s.listen(10) 

            while True:
                conn, addr = s.accept()
                data = conn.recv(1024).decode().split(',')

                threading.Thread(target=self.data_receive, args=(conn, addr, data)).start()

    def metrics(self):
        thresh_metric = [[] for i in range(5)]
        for query in self.query:
            if 'why does the compressibility transformation fail to correlate the' in self.query[query][0]: break
            responses = self.search(self.query[query][0])
            data = responses.decode().split('&&&')
            docs = []
            for d in data:
                if d == '': continue
                id = d.split(',')[0]
                docs.append(self.changes[id])
            print(f'received:\n{docs}\nexpected:\n{self.query[query][1]}')
            vals = Evaluations(docs, self.query[query][1])
            print(vals)
            for i in range(5): thresh_metric[i].append(vals[i])
        return [sum(arr)/len(arr) for arr in thresh_metric]



def Evaluations(docs, relevants):
    if len(docs) == 0:
        return [0, 0, 0, 0, 0]

    precision_value = precision(relevants, docs)
    recall_value = recall(relevants, docs)
    f_value=f_measure(precision_value,recall_value)
    r_precision_value=r_precision(relevants,docs)
    failure_value=failure_ratio(relevants,docs)
    return [ precision_value, recall_value, f_value, r_precision_value, failure_value ]

def precision(relevants, recovered):
    if len(recovered) == 0:
        return 0
    return len(set(relevants).intersection(set(recovered))) / len(recovered)

def recall(relevants, recovered):
    if len(relevants) == 0:
        return 0
    return len(set(relevants).intersection(set(recovered))) / len(relevants)

def f_measure(prec, rec):
    if prec + rec == 0:
        return 0
    return (2 * prec * rec) / (prec + rec)

def r_precision(relevants, recovered):
    r = len(relevants)
    if len(recovered) <= r:
        return len(set(recovered).intersection(set(relevants))) / len(relevants)
    return len(set(recovered[:r]).intersection(set(relevants))) / r

def failure_ratio(relevants, recovered):
    docs_irrelevant_recovered = set(recovered) - set(relevants)
    docs_irrelevant_total = 1399 - len(relevants)
    return len(docs_irrelevant_recovered) / docs_irrelevant_total


host_name = socket.gethostname() 
ip = socket.gethostbyname(host_name)
client = MetricNode(ip)