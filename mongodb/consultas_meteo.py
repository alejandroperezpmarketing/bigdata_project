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
db.meteo.find({
  nombre: { $regex: "França", $options: "i" },
  "valores": {
    $elemMatch: {
      "fecha": ISODate("2024-05-15T12:00:00Z")
    }
  }
}, {
  "_id": 0,
  "nombre": 1,
  "coordenadas": 1,
  "valores.$": 1  
}).pretty()


#########################################
#2.7)
""" 
Buscar el valor de NO2 de la Avenida de Francia
para cada una de las 24 horas del día 15 de mayo,
en la salida deben figurar únicamente el identificador 
de la estación, las coordenadas y los valores de fecha y NO2.  

  """
  db.meteo.aggregate([
  { 
    $match: { 
      nombre: { $regex: "França", $options: "i" }
    }
  },
  {
    $unwind: "$valores" 
  },
  {
    $match: { 
      "valores.fecha": { 
        $gte: ISODate("2024-05-15T00:00:00Z"),
        $lt: ISODate("2024-05-16T00:00:00Z")
      }
    }
  },
  {
    $project: {
      _id: 0,
      nombre: 1,
      valores: 1,
      coordenadas: 1
    }
  },
  {
    $sort: { "valores.fecha": 1 }
  }
]).forEach(doc => printjson(doc));


###########################################################################
#2.8)
# 2.8 (1 punto). Obtener el valor medio de
# NO2 por día del mes para 
# la estación Centre, exportar la consulta
# a un fichero .csv y dibujar
# en una gráfica el resultado
# (usando LibreOffice, por ejemplo, dentro de la máquina virtual). 
# Como ejemplo, para comprobar la solución,
# para el primer día del mes el resultado es 16.125. 
db.meteo.aggregate([
  { 
    $match: { nombre: { $regex: "Centre", $options: "i" } } 
  },
  { 
    $unwind: "$valores" 
  },
  { 
    $group: {
      _id: { 
        dia: { $dayOfMonth: "$valores.fecha" }, 
        nombre: "$nombre" 
      },
      meanNO2: { $avg: "$valores.NO2" }
    }
  },
  { 
    $sort: { "_id.dia": 1 } 
  },
  { 
    $project: {
      _id: 0,
      dia: "$_id.dia",
      nombre: "$_id.nombre",
      meanNO2: {$round:["$meanNO2", 2]}
    }
  },
  {
    $out: "meanNO2centrebyday"
  }
  
]).forEach(doc => printjson(doc));

#https://stackoverflow.com/questions/54754674/create-a-new-collection-from-other-collection-in-mongodb
######################################################
#2.9)
docker exec -it mongodb_bigdata_01 mongodump --db bigdata --collection meteo --out /tmp/dump --username vagrant --password vagrant --authenticationDatabase admin

sudo docker cp mongodb_bigdata_01:/tmp/dump ./dump_host

#coneccion con mongo atlas
docker exec -it mongodb_bigdata_01 mongo --host "mongodb+srv://clusterbigdata.umnae.mongodb.net" --username sakacel2 --password Scoutalex1995_ --authenticationDatabase admin
docker exec -it mongodb_bigdata_01 bash
bsondump /tmp/dump/bigdata/meteo.bson > /tmp/dump/bigdata/meteo.json
docker cp mongodb_bigdata_01:/tmp/dump/bigdata/meteo.json ./dump_host

docker exec -it mongodb_bigdata_01 mongoimport --uri  mongodb+srv://sakacel2:Scoutalex1995_@clusterbigdata.umnae.mongodb.net/bigdata  --collection  meteo  --type  JSON  --file  tmp/dump/bigdata/
meteo.json #import json file  to monogo atlas


##########################################################
#2.7)
# para la estación Avda. Francia y el mes de mayo la fecha exacta de mayor 
# y menor valor de NO2, la salida debe contener únicamente el nombre de la 
# estación y los valores de fecha y NO2, el resultado se puede obtener a 
# partir de dos consultas, 
# una para el valor máximo y otra para el valor mínimo.
db.meteo.aggregate([
  {
    $match: {
      nombre: { $regex: "França", $options: "i" }  // Filtra documentos por nombre
    }
  },
  {
    $unwind: "$valores"  // Descompón el array de "valores" para cada tupla de NO2 y fecha
  },
  {
    $sort: { "valores.NO2": 1 }  // Orden de los documentos por NO2 (de menor a mayor)
  },
  {
    $group: {
      _id: null,
      nombre: { $first: "$nombre" },  // Toma el primer valor de "nombre"
      min: { $first: { NO2: "$valores.NO2", fecha: "$valores.fecha" } },  // Primer valor (mínimo)
      max: { $last: { NO2: "$valores.NO2", fecha: "$valores.fecha" } }     // Último valor (máximo)
    }
  },
  {
    $project: {
      _id: 0,
      nombre: 1,
      min: 1,
      max: 1
    }
  }
]).pretty()
