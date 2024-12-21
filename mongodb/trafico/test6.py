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
    # Define the absolute path for the log directory
    logs_directory = "trafico/logs"
    log_file_path = os.path.join(logs_directory, 'trafic_log.log')
    
    # Verify if this directory exists
    if not os.path.exists(logs_directory):
        os.makedirs(logs_directory)
        
    # Set up a specific logger for the application
    logger = logging.getLogger("trafic_logger")
    logger.setLevel(logging.INFO)  # Use DEBUG for more verbose logging
    
    # Create a file handler to write logs to the log file
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)
    
    # Create a stream handler to output logs to the console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Define the log format
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # Log initialization
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

def extractdata(logger):
    # Set the paths for your data directories
    path = "./data/test"
    trafic_folder_name = 'trafico'
    df_trafic_path = os.path.join(path, trafic_folder_name)

    # Check if the traffic directory contains any files
    if not os.path.exists(df_trafic_path):
        logger.error(f"ERROR: Path '{df_trafic_path}' does not exist.")
        return
    elif not os.listdir(df_trafic_path):
        logger.error("ERROR: No traffic data files in folder available")
        return
    else:
        logger.info("ok trafic")
        count_trafic = len(os.listdir(df_trafic_path))
        logger.info(f'Number of files in directory: {count_trafic}')

        # Loop through each file in the traffic directory
        for i, y in enumerate(os.listdir(df_trafic_path)):
            try:
                trafic_file_path = os.path.join(df_trafic_path, y)
                logger.info(f"Processing file: {trafic_file_path}")

                # Parse the date from filename
                date_parts = y.split(".")[0]
                try:
                    parsed_date = datetime.strptime(date_parts.replace('H', ':').replace('m', ''), "%Y-%m-%dT%H:%M")
                except ValueError:
                    logger.error(f"Filename date format incorrect in line {id_line}: {trafic_file_path}")
                    continue
                
                # Convert to the Madrid timezone (UTC+1 or UTC+2 depending on DST)
                madrid_tz = pytz.timezone("Europe/Madrid")
                localized_date = madrid_tz.localize(parsed_date, is_dst=None)
                # Ensure it's stored as a UTC timestamp in MongoDB
                utc_date = localized_date.astimezone(pytz.utc) 
                iso_date = localized_date.isoformat()

                # Process each line in the traffic file
                with open(trafic_file_path, 'r', encoding="windows-1252") as file:
                    for id_line, line in enumerate(file):
                        try:
                            lect_linea = line.split(";")
                            logger.debug(f"Parsed line content: {lect_linea}")
                            id_tramo = str(lect_linea[1])
                            geo_point = lect_linea[-1]
                            
                            # Check and parse geo_point
                            latitude, longitude = None, None
                            if "," in geo_point:
                                latitude, longitude = map(str.strip, geo_point.replace(" ", "").split(","))
                                if latitude and longitude:
                                    coordinates = [latitude, longitude]
                                    logger.info(f'The following station {id_tramo} has the coordinates: {coordinates}')
                                    
                            else:
                                logger.info(f"Invalid geo_point_2d value in {trafic_file_path}: {geo_point}")

                            # Parse 'lectura' value
                            lectura_value = lect_linea[2].strip()
                            if lectura_value.replace('.', '', 1).isnumeric():
                                lectura = float(lectura_value)
                            else:
                                logger.error(f"Invalid lectura value in {trafic_file_path} on line {id_line}: {lectura_value}")
                                continue  # Skip invalid 'lectura' value
                            
                            
                            direccion = str(lect_linea[3]).lower()
                            estado = float(lect_linea[4])

                            # Define dictionary entry based on conditions
                            if lectura == -1 or lectura > 5000 or estado != 0:
                                lectura = None
                                # dictionary = [{"fecha": parsed_date.replace(tzinfo=madrid_tz).isoformat(), "lectura": lectura}]
                                # logger.info(f"Data entry created: {dictionary}")

                            else:
                                # dictionary = [{"fecha": parsed_date.replace(tzinfo=madrid_tz).isoformat(), "lectura": lectura}]
                                # logger.info(f"Data entry created: {dictionary}")
                                lectura=lectura
                            
                            #########################
                            # create document on MONGODB
                            # Upsert data to MongoDB
                            # Replace iso_date with localized_date in the MongoDB document to store it as a datetime object
                            # MongoDB Update Operation
                            result = db.trafico.update_one(
                                    {"id_tramo": id_tramo},  # Filter by id_estacion
                                    {
                                        "$push": {
                                            "valores": {
                                                "fecha": parsed_date.replace(tzinfo=madrid_tz),  # Store as ISODate
                                                "lectura": lectura
                                            }
                                        },
                                        "$setOnInsert": {  # Sets these fields only if a new document is created
                                            "id_tramo": str(id_tramo),
                                            "coordenadas": [float(latitude),float(longitude)],                                                "coordenadas": [latitude, longitude],
                                            "direccion": str(direccion)
                                        }
                                    },
                                    upsert=True  # Insert if not found
                                )
                                    
                            if result.upserted_id:
                                logger.info(f"A new document created with id_estacion: {id_tramo}")
                            else:
                                logger.info(f"Added new entry for existing document id_estacion: {id_tramo}")

                        except Exception as e:
                            logger.error(f"ERROR processing line {id_line} in {trafic_file_path}: {e}")
                            
            except Exception as e:
                logger.error(f"ERROR reading {trafic_file_path}: {e}")

def main():
    # Initialize the logger
    logger, logs_directory, _ = log_definition()
    
    # Run data extraction
    extractdata(logger)
    
    # Retrieve logs
    get_logs(logs_directory)

main()
