import sqlite3
import os
import json
from joblib import load, dump
from gensim.corpora import Dictionary

from database_controller.database_interface import Controller
from searcher.preprocess import data_processing

#import logging
#logging.basicConfig(level=logging.DEBUG,
#                    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
#
#logger = logging.getLogger(__name__)

def read_or_create_joblib(ip):
    ip = str(ip)
    os.makedirs(f"src/data/{ip}/", exist_ok=True)

    if os.path.exists(f"src/data/{ip}/dictionary.joblib"):
        return load(f"src/data/{ip}/dictionary.joblib")
    else:
        dump(Dictionary(), f"src/data/{ip}/dictionary.joblib")
        return Dictionary()

class DocumentController(Controller):
    dictionary: Dictionary
    
    def __init__(self, ip):
        self.ip = ip
        DocumentController.dictionary = read_or_create_joblib(ip)

    def connect(self):
        return sqlite3.connect(f"src/data/{self.ip}/database.db")

    

    def create_document(self, id, text, clock, table):
        try:
            if table == 'documentos':
                tokens = data_processing.tokenize_corpus([text])
                DocumentController.dictionary.add_documents(tokens)
                tf = DocumentController.dictionary.doc2bow(tokens[0])

                tf_json = json.dumps(tf)

                conn = self.connect()
                cursor = conn.cursor()


                cursor.execute(f'''
                    INSERT INTO {table} (id, text, clock, tf) VALUES (?, ?, ?, ?)
                ''', (id, text, clock, tf_json))
                conn.commit()
                conn.close()

                dump(DocumentController.dictionary, f"src/data/{self.ip}/dictionary.joblib")
            else:
                conn = self.connect()
                cursor = conn.cursor()

                cursor.execute(f'''
                    INSERT INTO {table} (id, text, clock) VALUES (?, ?, ?)
                ''', (id, text, clock))
                conn.commit()
                conn.close()
        except:
            pass


    def get_documents(self, table):
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(f'SELECT * FROM {table}')
        docs = cursor.fetchall()
        conn.close()
        
        return docs
    
    def get_document_by_id(self, _id, table = 'documentos'):
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute(f'SELECT text, clock FROM {table} WHERE id = ?', (_id,))
            doc = cursor.fetchone()
            conn.close()
            return doc[0], doc[1]
        except:
            return None
    
    def get_documents_for_query(self):
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute('SELECT id, tf FROM documentos')
        doc = cursor.fetchall()
        conn.close()
        
        return doc

    def update_document(self, id, clock, table, text=None):
        conn = self.connect()
        cursor = conn.cursor()

        if table == 'documentos':
            
            if text is not None:
                cursor.execute(f'SELECT text FROM {table} WHERE id = ?', (id,))
                doc = cursor.fetchone()[0]
                
                tokens = data_processing.tokenize_corpus([doc])
                
                bow = DocumentController.dictionary.doc2bow(tokens[0])

                for word, count in bow:
                    DocumentController.dictionary.cfs[word] -= count
                    DocumentController.dictionary.dfs[word] -= 1
                cursor.execute(f'''
                    UPDATE {table} SET text = ?, clock = ? WHERE id = ?
                ''', (text, clock, id))
            
            tokens_text = data_processing.tokenize_corpus([text])
            tf = DocumentController.dictionary.doc2bow(tokens_text[0])
            tf_json = json.dumps(tf)
            
            
            if tf_json is not None:
                cursor.execute(f'''
                    UPDATE {table} SET tf = ? WHERE id = ?
                ''', (tf_json, id))
            
            DocumentController.dictionary.add_documents(tokens_text)
            dump(DocumentController.dictionary, 'dictionary.joblib')

        else:
            cursor.execute(f'''
                    UPDATE {table} SET text = ?, clock = ? WHERE id = ?
                ''', (text, clock, id))
            
        conn.commit()
        conn.close()


    def delete_document(self, id, table):
        conn = self.connect()
        cursor = conn.cursor()

        if table == 'documentos':
            cursor.execute(f'SELECT text FROM {table} WHERE id = ?', (id,))
            
            doc = cursor.fetchone()[0]
            tokens = data_processing.tokenize_corpus([doc])
            
            bow = DocumentController.dictionary.doc2bow(tokens[0])
            
            for word, count in bow:
                DocumentController.dictionary.cfs[word] -= count
                DocumentController.dictionary.dfs[word] -= 1
            
            cursor = conn.cursor()
            cursor.execute(f'DELETE FROM {table} WHERE id = ?', (id,))

        else:
            cursor.execute(f'DELETE FROM {table} WHERE id = ?', (id,))

        conn.commit()
        conn.close()
        dump(DocumentController.dictionary, 'dictionary.joblib')

    def delete_all_documents(self):
        conn = self.connect()
        cursor = conn.cursor()

        # Eliminar todos los documentos
        cursor.execute('DELETE FROM documentos')

        # Actualizar el diccionario después de eliminar los documentos
        DocumentController.dictionary.clear()
        dump(DocumentController.dictionary, 'dictionary.joblib')

        conn.commit()
        conn.close()

    #def create_document(self, id, text, table):
    #    try:
    #        if table == 'documentos':
    #            tokens = data_processing.tokenize_corpus([text])
    #            DocumentController.dictionary.add_documents(tokens)
    #            tf = DocumentController.dictionary.doc2bow(tokens[0])
    #
    #            tf_json = json.dumps(tf)
    #
    #            conn = self.connect()
    #            cursor = conn.cursor()
    #
    #
    #            cursor.execute(f'''
    #                INSERT INTO {table} (id, text, tf) VALUES (?, ?, ?)
    #            ''', (id, text, tf_json))
    #            conn.commit()
    #            conn.close()
    #
    #            dump(DocumentController.dictionary, f"src/data/{self.ip}/dictionary.joblib")
    #        else:
    #            conn = self.connect()
    #            cursor = conn.cursor()
    #
    #            cursor.execute(f'''
    #                INSERT INTO {table} (id, text) VALUES (?, ?)
    #            ''', (id, text))
    #            conn.commit()
    #            conn.close()
    #    except:
    #        pass

    #def update_document(self, id, table, text=None):
    #    conn = self.connect()
    #    cursor = conn.cursor()
    #
    #    if table == 'documentos':
    #        
    #        if text is not None:
    #            cursor.execute(f'SELECT text FROM {table} WHERE id = ?', (id,))
    #            doc = cursor.fetchone()[0]
    #            
    #            tokens = data_processing.tokenize_corpus([doc])
    #            
    #            bow = DocumentController.dictionary.doc2bow(tokens[0])
    #
    #            for word, count in bow:
    #                DocumentController.dictionary.cfs[word] -= count
    #                DocumentController.dictionary.dfs[word] -= 1
    #            cursor.execute(f'''
    #                UPDATE {table} SET text = ? WHERE id = ?
    #            ''', (text, id))
    #        
    #        tokens_text = data_processing.tokenize_corpus([text])
    #        tf = DocumentController.dictionary.doc2bow(tokens_text[0])
    #        tf_json = json.dumps(tf)
    #        
    #        
    #        if tf_json is not None:
    #            cursor.execute(f'''
    #                UPDATE {table} SET tf = ? WHERE id = ?
    #            ''', (tf_json, id))
    #        
    #        DocumentController.dictionary.add_documents(tokens_text)
    #        dump(DocumentController.dictionary, 'dictionary.joblib')
    #
    #    else:
    #        cursor.execute(f'''
    #                UPDATE {table} SET text = ? WHERE id = ?
    #            ''', (text, id))
    #        
    #    conn.commit()
    #    conn.close()
