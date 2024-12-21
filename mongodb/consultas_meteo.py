import pymongo as pmo
from pymongo import MongoClient as mc
from bson.json_util import dumps
import json
%matplotlib inline
import matplotlib.pyplot as plt 



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
    $unwind: "$valores" 
  },
  { 
    $match: { nombre: { $regex: "Centre", $options: "i" },
             $expr: { $eq: [{$month: "$valores.fecha" }, 5]
             }}}
  ,
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
    $sort: { "_id.dia": -1 } 
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

#export in .csv
docker exec -it mongodb_bigdata_01 mongoexport --db=bigdata --collection=meanNO2centrebyday --type=csv --fields=nombre,dia,meanNO2 --out=/tmp/meanNO2centrebyday.csv --username vagrant --password vagrant --authenticationDatabase admin
sudo docker cp mongodb_bigdata_01:/tmp/ ./mongodb/data/consults

docker exec -it mongodb_bigdata_01 mongoexport --db=bigdata --collection=meanNO2centrebyday --type=csv --fields=nombre,dia,meanNO2 --out=/tmp/meanNO2centrebyday.csv --username vagrant --password vagrant --authenticationDatabase admin


import matplotlib.pyplot as plt
import numpy as np

# Inicializar listas para las categorías y los valores
categories = []
values = []

# Leer el archivo CSV utilizando open()
with open('./mongodb/data/consults/tmp/meanNO2centrebyday.csv', 'r', newline='') as file:
    # Omitir el encabezado
    next(file)
    for line in file:
        nombre, dia, meanNO2 = line.strip().split(',')
        categories.append(int(dia))  # Convertir el día a entero
        values.append(float(meanNO2))

# Ordenar por el valor de día (de 1 a 31)
sorted_data = sorted(zip(categories, values))
categories_sorted, values_sorted = zip(*sorted_data)

# Crear una lista de días del 1 al 31
all_days = list(range(1, 32))

# Asegurarse de que todos los días del 1 al 31 están representados
full_values = [0] * 31  # Inicializar con ceros
for day, value in zip(categories_sorted, values_sorted):
    full_values[day - 1] = value  # Asignar el valor correspondiente al día

# NO2 min and max
min_index = full_values.index(min(full_values))
max_index = full_values.index(max(full_values))

colors = ['grey'] * len(full_values)
colors[min_index] = 'green'
colors[max_index] = 'red'

# Crear el gráfico de barras
plt.bar(all_days, full_values, color=colors)

# Ajustar la visibilidad de las etiquetas del eje X (todos los días)
plt.xlabel('Day', fontsize='15')
plt.ylabel('NO2 level', fontsize='15')
plt.title('NO2 values in Valencia in May 2024', fontsize='18')

# Ajustar los ticks
plt.xticks(all_days, fontsize='10', rotation=0)

# Ajustar los datos para la tendencia (línea)
x_values = np.array(all_days)  # Días
y_values = np.array(full_values)  # Niveles de NO2

# Ajuste polinómico de primer grado (línea recta)
slope, intercept = np.polyfit(x_values, y_values, 1)
trendline = slope * x_values + intercept

# Graficar la línea de tendencia
plt.plot(x_values, trendline, color='blue', linestyle='--', label='Trendline')

# Mostrar la leyenda
plt.legend()

# Ajustar el layout para evitar solapamientos
plt.tight_layout()

# Mostrar el gráfico
plt.show()


#https://www.mongodb.com/docs/database-tools/mongoexport/mongoexport-examples/#std-label-mongoexport-fields-example
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
