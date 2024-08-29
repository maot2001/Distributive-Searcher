from gensim.models import TfidfModel
from searcher.preprocess import data_processing
from database_controller.controller_database import DocumentController
from gensim.similarities import MatrixSimilarity
from typing import List

class Retrieval_Vectorial():
    def __init__(self):
        pass
    
    def retrieve(self, query, controller: DocumentController):
        id_tf_documents = controller.get_documents_for_query()

        dictionary = controller.dictionary
        corpus: List = [bow for _, bow in id_tf_documents]

        all_list = [[eval(item)] for item in corpus]

        corpus = [item for sublist in all_list for item in sublist]

        model = TfidfModel(corpus)

        processed_query = data_processing.tokenize_corpus([query])[0]
        query_bow, missings = dictionary.doc2bow(processed_query, return_missing=True)

        # Inicializa un contador para los términos faltantes
        missing_count = {}

        # Itera sobre el BoW para identificar y contar los términos faltantes
        for term_id, freq in missings.items():
            if term_id not in dictionary.token2id:
                missing_count[term_id] = freq

        # Agrega los términos faltantes al BoW con un recuento de 0
        for term_id, freq in missing_count.items():
            query_bow.append((term_id, freq))

        # Calcular el TF-IDF para la consulta en relación con cada documento
        query_tfidf = model[query_bow]

        # Crear un índice de similitud a partir del corpus TF-IDF
        index = MatrixSimilarity(model[corpus])

        # Calcular la similitud de la consulta con cada documento
        sims = index[query_tfidf]

        # Filtrar documentos que tengan un TF-IDF mayor que el umbral especificado
        filtered_indices = [i for i, sim in enumerate(sims) if sim > 0]

        # Ordenar los documentos por similitud
        sorted_sims_indices = sorted(filtered_indices, key=lambda i: sims[i], reverse=True)

        # Obtener los IDs de los documentos más relevantes
        most_relevant_doc_ids = [id_tf_documents[i][0] for i in sorted_sims_indices]
        texts = [' '.join(controller.get_document_by_id(doc_id).split()[:40]) for doc_id in most_relevant_doc_ids]
        docs = [f'{doc_id},{text}' for doc_id, text in zip(most_relevant_doc_ids, texts)]

        return docs