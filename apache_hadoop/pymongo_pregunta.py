from pymongo import MongoClient as mc
import datetime
import json

client = mc('mongodb://vagrant:vagrant@localhost:27017/bigdata?authSource=admin')
db = client.bigdata


# 8) Realiza un programa en Python que genere la misma
# solución que la consulta anterior (usando funciones
# propias de Python no de MongoDB). La función de Python
# sort se debe usar para ordenar los resultados. 2 puntos.


#en terinal

consulta = db.meteo.aggregate([
    { 
        $unwind: "$valores" 
    },
    { 
        $group: {
            _id: "$nombre", 
            max_NO2: { $max: "$valores.NO2" }, 
            min_NO2: { $min: "$valores.NO2" } 
        }
    },
    { 
        $project: { 
            _id: 0, 
            nombre: "$_id", 
            max_NO2: 1, 
            min_NO2: 1 
        }
    },
    { 
        $sort: { nombre: 1 }
    }
]);


for valor in consulta:
    nombre = str(consulta.get("nombre", "unknown"))
    max_NO2 = int(consulta.get("max_NO2"))
    min_NO2 = int(consulta.get("min_NO2"))
    
    print(f'valores de cada estación Metereologicas: Nombre de la estación: {nombre}, Max NO2: {max_NO2}, Min NO2: {min_NO2}')


