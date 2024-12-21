# Author: Alejandro Perez Peñalver  
# This code extracts traffic data from .csv files, parses the information, and creates or updates a document in MongoDB.  
# Variables included: date, section_id, address, the number of vehicles at a specific time in that traffic station, and the station's coordinates (latitude, longitude).

import pandas as pd
import os
import json
from datetime import datetime
import pytz
import logging
import pymongo as pmo
from pymongo import MongoClient as mc


# Database connection

    #########################
    ###### MONGODB CONEXTION#############################
# 1. Client
#client = mc('mongodb://vagrant:vagrant@localhost:27017/bigdata')
client = mc('mongodb://vagrant:vagrant@localhost:27017/bigdata?authSource=admin')

db = client.bigdata
def log_definition():
    # The absolute path definition for the log directory
    logs_directory = "trafico/logs"
    log_file_path = os.path.join(logs_directory, 'trafico_logger_cambio_coordenadas.log')
    
    # Checking if this directory exists
    if not os.path.exists(logs_directory):
        os.makedirs(logs_directory)
        
    # Setting up a specific logger for the application
    logger = logging.getLogger("trafico_logger_cambio_coordenadas")
    logger.setLevel(logging.INFO)  # Use DEBUG for more verbose logging
    
    # Creating a file handler to write logs to the log file
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)
    
    # Creating a stream handler to output logs to the console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    #The log format definition
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Adding handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # Loggning initialization
    logger.info("Starting the script")
    return logger, logs_directory, log_file_path

def get_logs(logs_directory):
    log_file_path = os.path.join(logs_directory, 'trafic_log.log')

    if not os.path.exists(log_file_path):
        print("Log file does not exist.")
        return

    with open(log_file_path, 'r') as file:
        logs_content = file.read()
        print("Log file content:")
        print(logs_content)

logger, logs_directory, _ = log_definition()

collection = db.trafico

# Iterar sobre cada documento y cambiar el orden de latitud y longitud
for doc in collection.find():
    print(f"Documento original: {doc}")  # Imprimir el documento completo para ver la estructura

    # Verificar que 'coordenadas' es una lista de dos elementos
    if 'coordenadas' in doc and isinstance(doc['coordenadas'], list) and len(doc['coordenadas']) == 2:
        # Extraer los valores actuales
        #latitude, longitude = doc['coordenadas']
        longitude, latitude = doc['coordenadas']

        # Cambiar el orden a [longitude, latitude]
        #new_coords = [longitude, latitude]
        new_coords = [latitude,longitude]

        # Imprimir las coordenadas antes y después del cambio
        logger.info(f"Coordenadas antes: {doc['coordenadas']}")
        logger.info(f"Coordenadas después: {new_coords}")
        
        # Actualizar el documento en la base de datos
        result = collection.update_one(
            {'_id': doc['_id']},
            {'$set': {'coordenadas': new_coords}}
        )
        
        # Confirmar si se hizo la actualización
        if result.modified_count > 0:
            logger.info(f"Documento actualizado con ID: {doc['_id']}, Nuevas coordenadas: {new_coords}")
        else:
            logger.error(f"No se actualizó el documento con ID: {doc['_id']}")
    else:
        logger.info(f"El documento con ID {doc['_id']} no tiene 'coordenadas' como lista de dos elementos.")


