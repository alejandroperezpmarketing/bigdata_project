
#https://mrjob.readthedocs.io/en/latest/guides/quickstart.html#writing-your-first-job


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

from mrjob.job import MRJob
from datetime import datetime

class TrafficAnalysis(MRJob):
    def mapper(self, _, line):
        # Dividir la línea CSV en columnas
        columns = line.strip().split(',')
        if len(columns) == 3:
            id_tramo, fecha, valor = columns
            try:
                # Convertir fecha si es necesario
                fecha_obj = datetime.fromisoformat(fecha)
                fecha_str = fecha_obj.strftime('%Y-%m-%d %H:%M:%S')  # Convertir a string
            except ValueError:
                fecha_str = fecha  # Usar valor original si no es una fecha válida
            
            try:
                valor = int(float(valor))  # Convertir valor a entero
            except ValueError:
                valor = 0  # Asignar 0 si el valor no es numérico
            
            # Emitir el resultado como clave-valor
            yield id_tramo, (fecha_str, valor)

    def reducer(self, id_tramo, values):
        min_valor = float('inf')
        max_valor = float('-inf')
        fecha_min = None
        fecha_max = None

        for fecha, valor in values:
            if valor < min_valor:
                min_valor = valor
                fecha_min = fecha
            if valor > max_valor:
                max_valor = valor
                fecha_max = fecha

        # Emitir resultados con la clave del sensor
        yield id_tramo, {
            "min": {"fecha": fecha_min, "valor": min_valor},
            "max": {"fecha": fecha_max, "valor": max_valor},
        }

if __name__ == '__main__':
    TrafficAnalysis.run()
