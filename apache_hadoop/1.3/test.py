from pymongo import MongoClient as mc
from datetime import datetime

def extract():
    # Conexión a MongoDB
    client = mc('mongodb://vagrant:vagrant@localhost:27017/bigdata?authSource=admin')
    db = client.bigdata

    # Consulta para obtener los documentos
    trafico = db.trafico.find(
        {},
        {"_id": 0, "id_tramo": 1, "valores": 1}  # Solo selecciona los campos necesarios
    )

    # Preparar resultados
    result = []
    result.append("id_tramo,fecha,valor_transito")  # Cabecera del CSV, solo para mostrar en consola

    # Procesar cada documento
    for doc in trafico:
        id_tramo = str(doc.get('id_tramo', 'unknown'))  # Obtiene el nombre o un valor por defecto
        valores = doc.get('valores', [])  # Obtiene los valores o un array vacío
        
        # Iterar sobre los valores (asume que es un array de documentos)
        for item in valores:
            isodate = item.get('fecha')

            # Procesar fecha y valor
            if isinstance(isodate, datetime):  # Si ya es un objeto datetime
                fecha_str = isodate.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(isodate, str):  # Si es una cadena ISO
                fecha = datetime.fromisoformat(isodate)
                fecha_str = fecha.strftime('%Y-%m-%d %H:%M:%S')
            else:
                fecha_str = "unknown"

            # Comprobar si 'lectura' es None antes de convertir a float
            lectura = item.get('lectura')
            if lectura is None:
                valor_transito = 0.0  # Asignar 0.0 si 'lectura' es None
            else:
                valor_transito = int(float(lectura))  # Convertir a float si no es None

            # Agregar el resultado a la lista
            result.append(f'{id_tramo},{fecha_str},{valor_transito}')
    
    # Mostrar en consola los resultados
    for line in result:
        print(line)

# Ejecutar el mapper
mapper()
