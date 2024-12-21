from pymongo import MongoClient


# Conexión a la base de datos
client = MongoClient('mongodb://vagrant:vagrant@localhost:27017/bigdata?authSource=admin')
db = client.bigdata


resultados = db.meteo.find() 

estaciones = {}

for documento in resultados:
    nombre = documento.get("nombre")
    if "valores" in documento:
        for valor in documento["valores"]:
            no2 = valor.get("NO2")
            if no2 is not None:
                if nombre not in estaciones:
                    estaciones[nombre] = {"max_NO2": no2, "min_NO2": no2}
                else:
                    estaciones[nombre]["max_NO2"] = max(estaciones[nombre]["max_NO2"], no2)
                    estaciones[nombre]["min_NO2"] = min(estaciones[nombre]["min_NO2"], no2)

estaciones_lista = [{"nombre": nombre, "max_NO2": datos["max_NO2"], "min_NO2": datos["min_NO2"]} 
                    for nombre, datos in estaciones.items()]

estaciones_lista.sort(key=lambda x: x["nombre"])

for estacion in estaciones_lista:
    print(f'Nombre de la estación: {estacion["nombre"]}, Max NO2: {estacion["max_NO2"]}, Min NO2: {estacion["min_NO2"]}')

