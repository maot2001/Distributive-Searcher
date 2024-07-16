import sqlite3
import os
import json
from joblib import load, dump
from gensim.corpora import Dictionary


from database_controller.database_interface import Controller

from searcher.preprocess import prepro

def initialize_Database(ip):
    ip = str(ip)
    folder_path = 'src/data/nodes_data/'
    full_path = os.path.join(folder_path, ip)
    
    if os.path.exists(f"{full_path}/database.db"):
        print("El nodo ya había sido creado")
        return 
    
    else:
        # os.makedirs(full_path)
        # print.debug(f"Carpeta creada en: {full_path}")
        try:
            conexion = sqlite3.connect(os.path.join(full_path, 'database.db'))
            print("Exito al conectar la base de datos")
        except Exception as e:
            print(f"Error al conectar la base de datos: {e}")
            return
    
        cursor = conexion.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS documentos (
        	id INTEGER PRIMARY KEY,
        	texto_documento TEXT NOT NULL,
        	tf TEXT
        );
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS replica_succ (
        	id INTEGER PRIMARY KEY,
        	texto_documento TEXT NOT NULL,
        	tf TEXT
        );
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS replica_pred (
        	id INTEGER PRIMARY KEY,
        	texto_documento TEXT NOT NULL,
        	tf TEXT
        );
        ''')
        conexion.commit()
        conexion.close()
        print("La base de datos se creó exitosamente")    

def read_or_create_joblib(ip):
    ip = str(ip)
    """
    Intenta leer un archivo .joblib. Si no existe, crea uno con el objeto_predeterminado.
    :param nombre_archivo: Nombre del archivo .joblib a leer o crear.
    :param objeto_predeterminado: Objeto a guardar si el archivo no existe.
    :return: Contenido del archivo .joblib o el objeto_predeterminado.
    """
 
    
    if not os.path.exists(f"src/data/nodes_data/{ip}/"):
        url = f"src/data/nodes_data/{ip}/"
        print(f"Carpeta creada en: {url}")
        
        os.makedirs(f"src/data/nodes_data/{ip}/", exist_ok=True)
       
    
    if os.path.exists(f"src/data/nodes_data/{ip}/dictionary.joblib"):
        # El archivo existe, cargar y retornar su contenido
        print("EL joblib ya existe")
        return load(f"src/data/nodes_data/{ip}/dictionary.joblib")
    else:
        # El archivo no existe, crear uno nuevo con el objeto predeterminado
        dump(Dictionary(), f"src/data/nodes_data/{ip}/dictionary.joblib")
        print("EL joblib fue creado correctamente")
        return Dictionary()

class DocController(Controller):
    dictionary:Dictionary
    def __init__(self,ip):
        self.ip = ip
        DocController.dictionary = read_or_create_joblib(ip)
        
    def connect(self):
        return sqlite3.connect(f"src/data/nodes_data/{self.ip}/database.db")

    def create_document(self, texto_documento):
        tokens_documento = prepro.tokenize_corpus([texto_documento])
        DocController.dictionary.add_documents(tokens_documento)
        tf = DocController.dictionary.doc2bow(tokens_documento[0])

        # Convertir la lista tf a una cadena JSON
        tf_json = json.dumps(tf)

        # Conectar a la base de datos y verificar si la tabla existe
        conexion = self.connect()
        cursor = conexion.cursor()


        # Insertar los datos
        cursor.execute('''
            INSERT INTO documentos (texto_documento, tf) VALUES (?, ?)
        ''', (texto_documento, tf_json))
        conexion.commit()
        conexion.close()
        if self.leader:
            dump(DocController.dictionary, f"src/data/nodes_data/leader/dictionary.joblib")
        else:
            dump(DocController.dictionary, f"src/data/nodes_data/{self.ip}/dictionary.joblib")

        print("Diccionario actualizado y guardado.")  

    def get_documents(self):
        conexion = self.connect()
        cursor = conexion.cursor()
        cursor.execute('SELECT * FROM documentos')
        documentos = cursor.fetchall()
        conexion.close()
        # id2tok = { x: y for y, x in DocController.dictionary.token2id.items()}
        # print({ id2tok[k]:v for k,v in DocController.dictionary.cfs.items()})
        
        return documentos
    
    def get_document_by_id(self, _id):
        conexion = self.connect()
        cursor = conexion.cursor()
        cursor.execute('SELECT texto_documento FROM documentos WHERE id = ?', (_id,))
        documento = cursor.fetchone()
        conexion.close()
        return documento
    
    def get_documents_for_query(self):
        conexion = self.connect()
        cursor = conexion.cursor()
        cursor.execute('SELECT id, tf FROM documentos')
        documentos = cursor.fetchall()
        conexion.close()
        return documentos

    def update_document(self, id, texto_documento=None):
        conexion = self.connect()
        cursor = conexion.cursor()
        
        if texto_documento is not None:
            cursor.execute('SELECT texto_documento FROM documentos WHERE id = ?', (id,))
            documento = cursor.fetchone()[0]
            
            tokens_documento = prepro.tokenize_corpus([documento])
            
            bow = DocController.dictionary.doc2bow(tokens_documento[0])

            for word, count in bow:
                DocController.dictionary.cfs[word] -= count
                DocController.dictionary.dfs[word] -= 1
            cursor.execute('''
                UPDATE documentos SET texto_documento = ? WHERE id = ?
            ''', (texto_documento, id))
        
        tokens_texto_documento = prepro.tokenize_corpus([texto_documento])
        tf = DocController.dictionary.doc2bow(tokens_texto_documento[0])
        tf_json = json.dumps(tf)
        
        
        if tf_json is not None:
            cursor.execute('''
                UPDATE documentos SET tf = ? WHERE id = ?
            ''', (tf_json, id))
            
        conexion.commit()
        conexion.close()
        
        DocController.dictionary.add_documents(tokens_texto_documento)
        dump(DocController.dictionary, 'dictionary.joblib')

        print("Diccionario actualizado y guardado.")
    
    def delete_document(self, id):
        conexion = self.connect()
        cursor = conexion.cursor()
        cursor.execute('SELECT texto_documento FROM documentos WHERE id = ?', (id,))
        
        documento = cursor.fetchone()[0]
        tokens_documento = prepro.tokenize_corpus([documento])
        
        bow = DocController.dictionary.doc2bow(tokens_documento[0])
        
        for word, count in bow:
            DocController.dictionary.cfs[word] -= count
            DocController.dictionary.dfs[word] -= 1
        
        cursor = conexion.cursor()
        cursor.execute('DELETE FROM documentos WHERE id = ?', (id,))
        conexion.commit()
        conexion.close()
        dump(DocController.dictionary, 'dictionary.joblib')

        print("Diccionario actualizado y guardado.")
    
    def delete_all_documents(self):
        conexion = self.connect()
        cursor = conexion.cursor()

        # Eliminar todos los documentos
        cursor.execute('DELETE FROM documentos')

        # Actualizar el diccionario después de eliminar los documentos
        DocController.dictionary.clear()
        dump(DocController.dictionary, 'dictionary.joblib')

        conexion.commit()
        conexion.close()

        print("Todos los documentos eliminados y diccionario actualizado.")        

    
