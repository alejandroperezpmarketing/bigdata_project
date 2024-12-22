from pymongo import MongoClient
from datetime import datetime

def extract_data_to_file():
    # Conexión a MongoDB
    client = MongoClient('mongodb://vagrant:vagrant@localhost:27017/bigdata?authSource=admin')
    db = client.bigdata

    tramos = db.trafico.find({}, {"_id": 0, "id_tramo": 1, "valores": 1})

    with open('./trafico_data.csv', 'w') as file:
        file.write("id_tramo,fecha,valor\n")

        for tramo in tramos:
            id_tramo = tramo['id_tramo']
            for lectura in tramo['valores']:
                fecha = lectura['fecha']
                # Procesar fecha y valor
                if isinstance(fecha, datetime):  # Si ya es un objeto datetime
                    fecha_str = fecha.strftime('%Y-%m-%d %H:%M:%S')
                elif isinstance(fecha, str):  # Si es una cadena ISO
                    fecha = datetime.fromisoformat(fecha)
                    fecha_str = fecha.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    fecha_str = "unknown"
                valor = lectura['lectura']
                              
                
                file.write(f"{id_tramo},{fecha_str},{valor}\n")

    print("Archivo generado: trafico_data.csv")

extract_data_to_file()
