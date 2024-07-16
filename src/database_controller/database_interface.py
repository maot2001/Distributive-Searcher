from abc import ABC, abstractmethod


class Controller(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def create_document(self, texto_documento):
        pass

    @abstractmethod
    def update_document(self, id, texto_documento=None):
        pass

    @abstractmethod
    def delete_document(self, id):
        pass

    @abstractmethod
    def get_documents(self):
        pass

    @abstractmethod
    def get_document_by_id(self, _id):
        pass

    @abstractmethod
    def get_documents_for_query(self):
        pass

    