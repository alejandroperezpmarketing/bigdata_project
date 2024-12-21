from pymongo import MongoClient as mc
from mrjob.job import MRJob
from bson import json_util
from datetime import datetime

class Mongoextract(MRJob):
    def mapper(self, key, line):
        # MongoDB Connection
        client = mc('mongodb://vagrant:vagrant@localhost:27017/bigdata?authSource=admin')
        db = client.bigdata
        
        # Consultar la colección 'trafico'
        trafico = db.trafico.find(
            {},
            {"coordenadas": 0, "nombre": 1, "fecha": 1, "valores": 1}
        )
        
        # Procesar documentos
        for doc in trafico:
            # Extraer el campo fecha
            fecha_str = doc.get("fecha")
            if fecha_str:
                try:
                    # Convertir la fecha a un objeto datetime
                    fecha = datetime.fromisoformat(fecha_str)
                    dia = fecha.date()  # Obtener el día
                    hora = fecha.time()  # Obtener la hora
                except ValueError:
                    dia, hora = "invalid_date", "invalid_time"
            else:
                dia, hora = "no_date", "no_time"
            
            # Emitir resultado con día y hora incluidos
            yield doc.get("nombre", "unknown"), {
                "documento": json_util.dumps(doc),
                "dia": str(dia),
                "hora": str(hora)
            }

    def reducer(self, key, values):
        # Combinar los resultados del mapper
        yield key, list(values)

if __name__ == '__main__':
    Mongoextract.run()
