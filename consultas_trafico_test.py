import pymongo as pmo
from pymongo import MongoClient as mc
from bson.json_util import dumps
import json

client = mc('mongodb://vagrant:vagrant@localhost:27017/bigdata?authSource=admin')
db = client.bigdata

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


