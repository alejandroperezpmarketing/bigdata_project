from pymongo import MongoClient as mc
from mrjob.job import MRJob
from bson import json_util


class Mongoextract(MRjob):
    def mappper(self):
                
        # MongoDB Connection
        client = mc('mongodb://vagrant:vagrant@localhost:27017/bigdata?authSource=admin')
        db = client.bigdata
        

        trafico = db.meteo.find(
            {"valor":"$valores.lectura", "fecha":"$valores.fecha"},
            {"coordenadas": 0, "nombre": 1, "fecha":1, "valores":1}  
        )

        # Mostrar los resultados
        for doc in trafico:
            print(doc)

        
    def reducer(self):



if __name__ == '__main__':
    Mongoextract.run()