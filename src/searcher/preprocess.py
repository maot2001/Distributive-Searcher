import os
from typing import List, Tuple
from gensim.corpora import Dictionary
import spacy

nlp = spacy.load('database_controller/spacy_data/en_core_web_sm')

class data_processing:
    @staticmethod
    def tokenize(text):  
        doc = nlp(text)
        tokens = []
        for token in doc:
            if token.text.isalpha(): 
                tokens.append(token.lemma_.lower())
    
        return tokens
    
    @staticmethod
    def tokenize_corpus(corpus: List[str]) -> List[List[str]]:
        return [data_processing.tokenize(doc) for doc in corpus]
    
    @staticmethod
    def get_dictionary(corpus : List[List[str]]) -> Dictionary:
        return Dictionary(corpus)
    
    @staticmethod
    def get_bow_corpus(corpus : List[List[str]], dictionary : Dictionary) -> List[List[Tuple[int, int]]]:
        return [dictionary.doc2bow(doc) for doc in corpus]
    
