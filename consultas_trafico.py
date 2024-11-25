import pymongo as pmo
from pymongo import MongoClient as mc
from bson.json_util import dumps
import json
client = mc('mongodb://vagrant:vagrant@localhost:27017/bigdata?authSource=admin')
db = client.bigdata


#################################################################
""" from bson import json_util

meteo_centre = db.meteo.find(
    {"nombre": {"$regex": "centre", "$options": "i"}},  # Buscar que 'nombre' contenga 'centre'
    {"coordenadas": 1, "nombre": 0}  # Incluir solo 'coordenadas' y excluir 'nombre' y '_id'
)

# Mostrar los resultados
for doc in meteo_centre:
    print(doc)
 """

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


"""  
VERIFICACIONES


arrays = db.trafico_test.find({"valores.fecha": {"$type": "array"}})
for i in arrays:
    print(i)
    
# Filtro lo documentos donde `valores.fecha` no es de tipo `Date`
    non_date_records = db.trafico_test.find({"valores.fecha": {"$not": {"$type": "date"}}})
    for record in non_date_records:
        print(record) """
################################################################


################################################################
#2.1.1)

try:
    #2.1.1)
    #print("ok mongo")
    tramos = db.trafico.count_documents({})
    print(f"El número de tramos de medida de intensidad de tráfico es: {tramos}")
    
    #en terminal 
    db.trafico.count()
    
    
    
    #2.1.2       
    # se busca todos los documentos, y se muestra los tipos de `valores.fecha`
    documents = db.trafico_test.find({}, {"valores.fecha": 1})
    for doc in documents:
        print(dumps(doc))
        
except pmo.errors.PyMongoError as e:
    print(f"An error occurred: {e}")

    #en terminal
    db.trafico.findOne({},{valores:{$slice:1}})

################################################################


################################################################
#2.2)    
try:

    # Paso 1: Despliegue del array `valores` para trabajar con cada entrada individualmente
    step1 = {
        "$unwind": "$valores"
    }

    # Paso 2: Agrupación por `id_tramo` y mes, y calculo de la media de `lectura`
    step2 = {
        "$group": {
            "_id": {
                "id_tramo": "$id_tramo",                  # Se agrupar por `id_tramo`
                "month": {"$month": "$valores.fecha"}      # Se extrae el mes de la fecha
            },
            "media": {"$avg": "$valores.lectura"}         # se calcula la media de las lecturas
        }
    }

    # Paso 3: Orden por la media de forma descendente
    step3 = {
        "$sort": {"_id.id_tramo": -1}
    }

    # Ejecución de la agregación con los pasos
    result = db.trafico.aggregate([step1, step2, step3])

    # Se imprimen los resultados
    for res in result:
        print(f"Tramo: {res['_id']['id_tramo']}, Mes: {res['_id']['month']}, Media: {res['media']:.2f}")

except pmo.errors.PyMongoError as e:
    print(f"An error occurred: {e}")



#### en terminal

db.trafico.aggregate([
    {$unwind: "$valores"},
    {$group: {
      _id: {
        month: {$month:"$valores.fecha"},
        id_tramo: "$id_tramo"
      },
      media: {$avg:"$valores.lectura"}
    }
  },
    {$sort: {"_id.id_tramo":-1}}
])
##############################################################


################################################################
#2.3)    
""" try:
    
    step0_p_2_3 = {
        
        "$match": {"id_tramo": "A111"}
        
        
    }

    # Paso 1: Despliegue del array `valores` para trabajar con cada entrada individualmente
    step1_p_2_3 = {
        "$unwind": "$valores"
    }

    # Paso 2: Agrupación por `id_tramo` y mes, y calculo de la media de `lectura`
    step2_p_2_3 = {
        "$group": {
            "_id": {
                "id_tramo": "$id_tramo", # Agrupación por `id_tramo`
                "month": {"$month": "$valores.fecha"},     # Se extra el mes de la fecha
                "day": {"$dayOfMonth": "$valores.fecha"}  # Se extrae el día de la fecha

            },
            "media": {"$avg": "$valores.lectura"}         # se calcula la media de las lecturas
        }
    }

    # Paso 3: Orden por la media de forma descendente
    step3_p_2_3 = {
        "$sort": {"_id.day": -1}
    }

    # Ejecución la agregación con los pasos
    result = db.trafico.aggregate([step0_p_2_3, step1_p_2_3, step2_p_2_3, step3_p_2_3])

    # Se imprime los resultados
    for res in result:
        print(f"Tramo: {res['_id']['id_tramo']}, Day: {res['_id']['day']}, Mes: {res['_id']['month']}, Media: {res['media']:.2f}")

except pmo.errors.PyMongoError as e:
    print(f"An error occurred: {e}")

 """
#### en terminal

db.trafico.aggregate([
    {$match: {"id_tramo": "A111"}},
    {$unwind: "$valores"},
    {$group: {
      _id: {
        month: {$month:"$valores.fecha"},
        day: {$dayOfMonth: "$valores.fecha"}
      },
      media: {$avg:"$valores.lectura"}
    }
  },
    {$sort: {"_id.day":-1}}
])


##############################################################
##############################################################
#2.4)

# Paso 0 se crea un indice espacial para poder usar latitud y longitud del campo coordenadas 
#db.trafico.find_one({},{"valores.fecha":{"$slice":1}})
#db.trafico.drop_index("coordenadas_2dsphere")
#para consultas en terminal se usa lo siguiente db.trafico.create_index({"coordenadas":"2dsphere"})
#db.trafico.create_index([("coordenadas","2dsphere")])
# nombre del indice "coordenadas_1_2dsphere_1"
# coordenadas de la estación meteorológica Centre. "Centre": [39.470718, -0.376384

try:
    print("ok mongo")

    # Paso 0: Crear el índice espacial si aún no existe
    db.trafico.create_index([("coordenadas", "2dsphere")])
    
    # Coordenadas de la estación meteorológica "Centre"
    lat = 39.470718
    lng = -0.376384

    # Paso 1: Filtro espacial usando $geoNear (debe ser la primera etapa)
    step1_p_2_4 = {
        "$geoNear": {
            "near": { "type": "Point", "coordinates": [lng, lat] },
            "distanceField": "DistanciaCalculada",
            "maxDistance": 600,
            "spherical": True
        }
    }

    # Paso 2: Despliegue del array `valores` para trabajar con cada entrada individualmente
    step2_p_2_4 = {
        "$unwind": "$valores"
    }

    # Paso 3: Agrupar por mes y día, y calcular el promedio de la lectura
    step3_p_2_4 = {
        "$group": {
            "_id": {
                "month": {"$month": "$valores.fecha"},    # Se extrae el mes de la fecha
                "day": {"$dayOfMonth": "$valores.fecha"}  # Se extrae el día de la fecha
            },
            "media": {"$avg": "$valores.lectura"}        # Se calcula la media de las lecturas
        }
    }
    
    step4_p_2_4 = {
        "$sort": {"_id.day": -1}
    }

    # Ejecutar la consulta de agregación
    result_p_2_4 = db.trafico.aggregate([step1_p_2_4, step2_p_2_4, step3_p_2_4,step4_p_2_4])

    # Imprimir resultados
    for res in result_p_2_4:
        print(res)
        #print(f"Mes: {res['_id']['month']}, Día: {res['_id']['day']}, Media: {res['media']:.2f}")
        #print(f"Distancia Calculada: {res['DistanciaCalculada']}")
    
except pmo.errors.PyMongoError as e:
    print(f"An error occurred: {e}")


### en terminal

db.trafico.createIndex({"coordenadas":"2dsphere"}) # creacion del indice


db.trafico.aggregate([
    {$geoNear: {near: {"coordinates": [39.470718,-0.376384]},
            distanceField: "DistanciaCalculada",
            maxDistance: 600}},
    {$unwind: "$valores"},
    {$group: {
      _id: {
        month: {$month:"$valores.fecha"},
        day: {$dayOfMonth: "$valores.fecha"}
      },
      media: {$avg:"$valores.lectura"}
    }
  },
    {$sort: {"_id.day":-1}}
])


