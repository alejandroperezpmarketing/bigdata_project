import pymongo as pmo
from pymongo import MongoClient as mc
from bson.json_util import dumps
import json
client = mc('mongodb://vagrant:vagrant@localhost:27017/bigdata?authSource=admin')
db = client.bigdata



################################################################
#1)

# agregación para limitar el arreglo 'valores' a solo un elemento en el primer documento
documents = db.trafico.aggregate([
    {
        "$project": {
            "valores": { "$slice": ["$valores", 1] },  # se limita el arreglo 'valores' a un solo elemento
            "id_tramo":1, # Se agrega el campo '_id'
            "coordenadas":1,
        }
    },
    {
        "$limit": 1  # Se Limita los resultados a solo el primer documento
    }
])

# Mostrar el resultado
for doc in documents:
    print(dumps(doc))