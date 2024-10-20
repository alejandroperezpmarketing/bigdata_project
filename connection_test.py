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
    
    
    
    
    
    
#update a documento alrady created and if this documento does not exist create it
                             result = db.trafico.update_one({"id_tramo":id_tramo},{"$push":{"valores":{"fecha":date_parts},
                                                                                   "lectura":lectura}})
                            
                            if result.raw_result["nModified"] == 0:
                                db.trafico.insert_one({"id_tramo":id_tramo,
                                                       "direccion":direccion,
                                                        "coordenadas":[latitude,longitude],
                                                        "valores":dictionary})
                            """