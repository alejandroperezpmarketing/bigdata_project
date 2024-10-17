result = db.update_one({"id_tramo"=id_tramo},{"&push":{"valores":{"fecha":fecha,"lectura":lectura}}})

if result.raw_result["nModified"] == 0:
    db.insert_one({"id_tramo":id_tramo,
                   "direccion":direccion,
                   "coordenadas":coordenadas,
                   "valores":diccionario})
diccionario = {"fecha[:fecha,"lectura":]
               
               
               """ 
                    if lectura == -1 or lectura > 5000 or group['Estat / Estado'] != 0:

                        lectura = None

                    else:
                        lectura = lectura
 """