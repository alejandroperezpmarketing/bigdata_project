import pymongo as pmo
from pymongo import MongoClient as mc

client = mc('mongodb://vagrant:vagrant@localhost:27017/admin')
#client = mc('mongodb://vagrant:vagrant@localhost:27017/bigdata')

db = client.Test
coleccion = db.list_collections_names()

print(coleccion)
doc = coleccion.find()

for document in doc:
    
    print(document)