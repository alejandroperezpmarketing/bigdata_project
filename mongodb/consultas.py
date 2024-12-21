import pymongo as pmo
from pymongo import MongoClient as mc

# Database connection
client = mc('mongodb://vagrant:vagrant@localhost:27017/bigdata?authSource=admin')
db = client.bigdata

# 1. Count the number of traffic sections
number_of_traffic_sections = db.trafico.count_documents({})
print(number_of_traffic_sections)

# 2. Calculate the monthly average traffic intensity for each section
step1_p2 = {
    "$group": {
        "_id": {
            "id_tramo": "$id_tramo",  # Group by section id
            "month": {"$month": "$valores.fecha"}  # Extract month from the date
        },
        "media": {"$avg": "$valores.lectura"}  # Calculate the average of readings
    }
}

step2_p2 = {
    "$sort": {"media": -1}  # Sort by average in descending order
}

# Execute the aggregation
result = db.trafico.aggregate([step1_p2, step2_p2])

# Print results
for res in result:
    print(f"Tramo: {res['_id']['id_tramo']}, Month: {res['_id']['month']}, Average: {res['media']:.2f}")



# 3)

""" 
###################################### METEO

#Ordenar las estaciones meteorológicas por latitud de mayor a menor. 


estaciones_por_latitude = db.meteo.find().sort("coordenadas.0",-1)

for i in estaciones_por_latitude:
    print(f"Estacion : {i['nombre']}, coordenadas: {i['coordenadas']}")

Buscar el valor de NO2 para la Avenida de Francia para la fecha del 15 de mayo 
a las 12:00:00, en la salida deben figurar únicamente el identificador de la estación,
las coordenadas y los valores de fecha y NO2. 

pipeline = [
    {
        "$unwind": "$valores"  # Unwind the valores array to access individual entries
    },
    {
        "$match": {
            "id_estacion": "$id_estacion",  # Filter by station ID
            "$expr": {
                "$and": [
                    {"$eq": [{"$month": "$valores.fecha"}, 5]},  # Month must be May
                    {"$eq": [{"$hour": "$valores.fecha"}, 12]}  # Hour must be 12 PM
                ]
            },
            "NO2": { "$exists": True }  # Ensure NO2 field exists
        }
    },
    {
        "$project": {
            "id_estacion": 1,
            "valores.fecha": 1,
            "NO2": 1  # Include the NO2 field in the output
        }
    }
]

# Execute the aggregation pipeline
results = db.collection.aggregate(pipeline)

# Print the results
for result in results:
    print(result)
 """