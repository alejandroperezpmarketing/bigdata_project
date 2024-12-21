from pymongo import MongoClient as mc
import datetime

# 1.1 (1 punto). Escribir una función mapper para que lea, 
# directamente de la base de datos de MongoDB, para cada documento 
# de tráfico, el identificador de estación y el valor de tráfico para
# cada fecha (una línea para cada hora del mes para cada uno de los sensores).

def mapper():
                
    # MongoDB Connection
    client = mc('mongodb://vagrant:vagrant@localhost:27017/bigdata?authSource=admin')
    db = client.bigdata
        

    trafico = db.trafico.find(
        {},
        {"nombre": 1, "valores":1}  
    )

    result = []
    result.append('id'+','+'nombre'+","+"fecha"+"valor"+"\n")
    
     # Mostrar los resultados
    for doc in trafico:
        id = doc.get('_id')
        nombre = str(doc.get('nombre','unkwon'))
        valores = doc.get('valores', {})
        for isodate, valor in valores.items():
            fecha = datetime.fromisoformat(isodate)
            valor = round(float(valor),2)
            
            result.append(f'{id},{nombre},{fecha},{valor}\n')
    
    return result


lines = mapper()

for line in lines:
    print(line, end='')
    
    


