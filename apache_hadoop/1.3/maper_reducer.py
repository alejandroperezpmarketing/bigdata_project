
#https://mrjob.readthedocs.io/en/latest/guides/quickstart.html#writing-your-first-job

from pymongo import MongoClient as mc
from mrjob.job import MRJob

""" 
##############################
1.3 (1 punto). Escribe un programa en Python para que 
genere un fichero a partir de la base de datos de MongoDB 
donde figuren el identificador de tráfico, la fecha y el 
valor para cada lectura de cada sensor de tráfico,
##############################
sube este fichero a HDFS (ESTE SERÁ EL FICHERO AL QUE HACE REFERENCIA 
EL RESTO DE CONSULTAS). Utiliza la clase MRJob para escribir
un código que realice la operación de mapeo y reducción sobre
el fichero anterior grabado en HDFS necesaria para obtener, para
cada sensor de tráfico, los valores de temperatura máximo y mínimo
incluyendo la fecha (incluyendo día y hora) exacta en que se produjo
cada uno de estos valores. 
##############################
La salida debe ser por la terminal y estar
ordenada por sensor y los resultados máximo y mínimo se deben ver a la vez.
Como ejemplo, para comprobar la solución, para el sensor A10, el menor valor,
0, se produce el 13 de mayo a las 3:03 (en este caso es el primer cero que
encuentra de toda la serie) y el mayor se produce el 3 a las 14:00,
con un valor de 3210.
 """

class Mongoextract(MRjob):
    def mappper(self, key, line):
                
        # MongoDB Connection
        client = mc('mongodb://vagrant:vagrant@localhost:27017/bigdata?authSource=admin')
        db = client.bigdata
        
        trafico = db.trafico.aggregate([
            { "$unwind": "$valores" },  # Descompone el array "valores"
            { "$project": {  # Selecciona los campos deseados
                "_id": 0,  # Excluye el campo _id
                "id_tramo": 1,  # Incluye el campo id_tramo
                "fecha": "$valores.fecha",  # Incluye la fecha desde valores.fecha
                "valor": "$valores.valor"  # Incluye el valor desde valores.valor
                }}
            ])

        for doc in trafico:
            print(doc)

        
    def reducer(self):
        print("k")



if __name__ == '__main__':
    Mongoextract.run() 