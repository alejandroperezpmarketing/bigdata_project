# Author: Alejandro Perez Peñalver  
# This code extracts meteorological data from .txt files, parses the information, and creates or updates a document in MongoDB.  
# Variables included = date, id_station, NO2 value, station name, and the station's coordinates (latitude, longitude).


import os
import re
from datetime import datetime, time
from pymongo import MongoClient as mc
from zoneinfo import ZoneInfo
import logging

#The absolute path definition for the logs directory
logs_directory = "meteo/logs"  # Replace with an absolute path if needed
log_file_path = os.path.join(logs_directory, 'meteo_logs_test.log')

# Checking if logs directory exists
if not os.path.exists(logs_directory):
    os.makedirs(logs_directory)

# Setting up a specific logger for the application
logger = logging.getLogger("meteo_logger_test")
logger.setLevel(logging.INFO)  # Set to DEBUG to capture all messages

# file handler definition to write logs to file
file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(logging.INFO)

# Creating stream handler to output logs to console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Defining log format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Adding handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Log a test message
logger.info("Starting the script")

# Defining date patterns to match different date formats
patterns = [
    r"^\d{4}-\d{2}-\d{2}",    # YYYY-MM-DD
    r"^\d{2}/\d{2}/\d{4}",    # DD/MM/YYYY or MM/DD/YYYY
    r"^[A-Za-z]+\s\d{1,2},\s\d{4}"  # Month Day, Year
]

# MongoDB Connection
client = mc('mongodb://vagrant:vagrant@localhost:27017/bigdata?authSource=admin')
db = client.bigdata

# Coordinates Dictionary
coordinates = {
    "Av França": [39.457504, -0.342689],
    "Bulevard Sud": [39.450378, -0.396313],
    "Molí del Sol": [39.481138, -0.408558],
    "Pista de Silla": [39.458060, -0.376653],
    "Politècnic": [39.479621, -0.337407],
    "Centre": [39.470718, -0.376384],
    "Vivers": [39.479488, -0.369550],
    "València Port Moll Trans Ponent": [39.459264, -0.323217],
    "Nazaret Met-2": [39.448554, -0.333289],
    "Conselleria Meteo": [39.472034, -0.404878],
    "València Olivereta": [39.469238, -0.406037],
    "València Port llit antic Túria": [39.450518, -0.328945]
}

def contains_date(line):
    for i, pattern in enumerate(patterns):
        if re.search(pattern, line):
            return i
    return None

def find_coordinates_partial_match(estacion):
    estacion_words = set(estacion.split())
    for station_name, coords in coordinates.items():
        if estacion_words.intersection(station_name.split()):
            return coords
    return None

def extractdata_meteo():
    path = "/home/vagrant/Documents/bigdata/bigdata_project/meteo/"
    meteo_folder_name = 'estaciones_metereologicas'
    df_meteo_path = os.path.join(path, meteo_folder_name)
    madrid_tz = ZoneInfo("Europe/Madrid")

    files = os.listdir(df_meteo_path)
    if not files:
        logger.error("No traffic data files in folder available")
        return

    logger.info(f'Number of files in directory: {len(files)}')

    if len(files) == 12:
        logger.info("12 meteorological stations found in this directory")
        
        for file_index, file_name in enumerate(files):
            meteo_file_path = os.path.join(df_meteo_path, file_name)
            logger.info(f"Opening file: {meteo_file_path}")

            try:
                with open(meteo_file_path, 'r', encoding="iso-8859-1") as file:
                    lecture = file.readlines()
                    pos_NO2, pos_date, pos_hour = None, None, None
                    estacion = None
                    
                    for line in lecture:
                        lect_line = line.replace('\n', '').strip().split("\t")

                        if "Estaci" in lect_line[0] and not estacion:
                            parts = line.strip().split("-", 1)
                            estacion = parts[-1].strip().replace('.', '')
                            estacion = estacion[1:] if estacion.startswith(' ') else estacion
                            logger.info(f"Found station: {estacion}")

                            station_coordinates = find_coordinates_partial_match(estacion)
                            if not station_coordinates:
                                logger.warning(f"No coordinates found for '{estacion} {meteo_file_path}', skipping file.")
                                continue
                            latitude, longitude = station_coordinates

                            # Assigning id_estacion based on the station name
                            id_estacion = file_index + 1  # Adjust this as needed based on your logic

                        if "FECHA" in lect_line and "NO2" in lect_line:
                            pos_date = lect_line.index("FECHA")
                            pos_hour = lect_line.index("HORA")
                            pos_NO2 = lect_line.index("NO2")
                        

                        if pos_date is not None:
                            date_format_index = contains_date(lect_line[pos_date])
                            if date_format_index is not None:
                                try:
                                    date_value = lect_line[pos_date].replace(' ', '')

                                    if date_format_index == 0:
                                        date_obj = datetime.strptime(date_value, "%Y-%m-%d")        
                                    elif date_format_index == 1:
                                        date_obj = datetime.strptime(date_value, "%d/%m/%Y")
                                    elif date_format_index == 2:
                                        date_obj = datetime.strptime(date_value, "%B %d, %Y")

                                    if date_obj.month != 5:
                                        continue
                                    
                                    hour = int(lect_line[pos_hour]) if lect_line[pos_hour] else None
                                    combined_datetime = datetime.combine(date_obj, time(hour, 0))

                                    NO2_value = int(lect_line[pos_NO2]) if lect_line[pos_NO2] else None
                                    if NO2_value is None:
                                        continue

                                    # MongoDB Update Operation
                                    result = db.meteo_test.update_one(
                                        {"id_estacion": id_estacion},  # Filter by id_estacion
                                        {
                                            "$push": {
                                                "valores": {
                                                    "fecha": combined_datetime.replace(tzinfo=madrid_tz),  # Store as ISODate
                                                    "NO2": NO2_value
                                                }
                                            },
                                            "$setOnInsert": {  # Sets these fields only if a new document is created
                                                "nombre": estacion,
                                                "coordenadas": [latitude, longitude],
                                                "id_estacion": id_estacion  # Ensure id_estacion is set
                                            }
                                        },
                                        upsert=True  # Insert if not found
                                    )
                                    
                                    if result.upserted_id:
                                        logger.info(f"A new document created with id_estacion: {id_estacion}, name: {estacion}")
                                    else:
                                        logger.info(f"Added new entry for existing document id_estacion: {id_estacion}, name: {estacion}")

                                except Exception as e:
                                    logger.error(f'Error parsing date in file {file_name} line "{line}": {e}')
                            else:
                                logger.error(f"ERROR parsing date from {meteo_file_path}")
            except Exception as e:
                logger.error(f'Error processing file {file_name}: {e}')

extractdata_meteo()

# Function to read and display log contents
def get_logs(logs_directory):
    log_file_path = os.path.join(logs_directory, 'meteo_logs.log')

    if not os.path.exists(log_file_path):
        print("Log file does not exist.")
        return

    with open(log_file_path, 'r') as file:
        logs_content = file.read()
        print("Log file content:")
        print(logs_content)

# Retrieving and printting the log contents after data extraction
get_logs(logs_directory)
