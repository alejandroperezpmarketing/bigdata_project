import pymongo as pmo
from pymongo import MongoClient as mc
from bson.json_util import dumps
import json
client = mc('mongodb://vagrant:vagrant@localhost:27017/bigdata?authSource=admin')
db = client.bigdata



#########################################
#2.5)
##########################################
#en terminal

db.meteo.find(
    {},
    {
        nombre:1,  
    }
    ).sort({ "coordenadas.0":-1}).pretty()



""" 
#########################################
# agregación para limitar el arreglo 'valores' a solo un elemento en el primer documento
documents = db.trafico.aggregate([
    {
        "$project": {
            "valores": { "$slice": ["$valores", 1] },  # se limita el arreglo 'valores' a un solo elemento
            "nombre":1, # Se agrega el campo '_id'
            "coordenadas":1,
        }
    },
    {
        "$limit": 1  # Se Limita los resultados a solo el primer documento
    }
])

# Mostrar el resultado
for doc in documents:
    print(dumps(doc)) """
    
#########################################
#2.6)
""" Buscar el valor de NO2 para la Avenida de Francia para la fecha del 
15 de mayo a las 12:00:00, en la salida deben figurar únicamente 
el identificador de la estación, las coordenadas y los valores de fecha y NO2.  """



##########################################
#en terminal

db.meteo.find(
    {nombre:"Av França", $dayofMonth:{"valores.fecha":5},$time:{"valores.fecha":12:00:00}},
    {
        nombre:1,
        coordenadas:1,
        valores:1
    }
    ).pretty()


db.meteo.aggregate([
  {
    $match: {
      nombre: "Av França", // Filtrar por el nombre
      "valores.fecha": {
        $gte: new ISODate("2024-11-05T12:00:00Z"), // Asegúrate de ajustar el formato de fecha si es necesario
        $lt: new ISODate("2024-11-06T12:00:00Z")
      }
    }
  },
  {
    $project: {
      nombre: 1,
      coordenadas: 1,
      valores: 1,
      day: { $dayOfMonth: "$valores.fecha" }, // Extraer el día de la fecha
      hour: { $hour: "$valores.fecha" } // Extraer la hora de la fecha
    }
  },
  {
    $match: {
      day: 5, // Filtrar solo por el día 5
      hour: 12 // Filtrar solo por las 12:00:00
    }
  }
]).pretty();
