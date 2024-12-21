import pandas as pd
import os
import json
import pymongo as pmo
from pymongo import MongoClient as mc
from datetime import datetime
import logging
from zoneinfo import ZoneInfo

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection
client = mc('mongodb://vagrant:vagrant@localhost:27017/bigdata?authSource=admin')
db = client.bigdata

def process_file(trafic_file_path):
    """Process a single traffic data file."""
    try:
        # Extract date parts from filename
        date_parts = os.path.basename(trafic_file_path).split(".")[0]
        parsed_date = datetime.strptime(date_parts.replace('H', ':').replace('m', ''), "%Y-%m-%dT%H:%M")
        iso_date = parsed_date.strftime("%Y-%m-%dT%H:%M")

        with open(trafic_file_path, 'r', encoding="windows-1252") as file:
            for line in file:
                lect_linea = line.split(";")
                if len(lect_linea) < 5:
                    logging.warning(f"Skipping incomplete line: {line.strip()}")
                    continue

                id_tramo = str(lect_linea[1])
                geo_point = lect_linea[-1]
                latitude, longitude = parse_geo_point(geo_point)

                lectura_value = lect_linea[2].strip()
                if not lectura_value.replace('.', '', 1).isnumeric():
                    logging.warning(f"Invalid lectura value in {trafic_file_path}: {lectura_value}")
                    continue

                lectura = float(lectura_value)
                direccion = str(lect_linea[3]).lower()
                estado = float(lect_linea[4])

                document = {
                    "id_tramo": id_tramo,
                    "direccion": direccion,
                    "coordenadas": [latitude, longitude],
                    "valores": [{
                        "fecha": iso_date,
                        "lectura": lectura if lectura != -1 and lectura <= 5000 and estado == 0 else None
                    }]
                }

                upsert_document(document)

    except Exception as e:
        logging.error(f"ERROR processing {trafic_file_path}: {e}")

def parse_geo_point(geo_point):
    """Parse the geographical point from the string."""
    if "," in geo_point:
        latitude, longitude = map(str.strip, geo_point.split(","))
        return float(latitude), float(longitude)
    else:
        logging.warning(f"Invalid geo_point_2d value: {geo_point}")
        return None, None

def upsert_document(document):
    """Insert or update a document in MongoDB."""
    try:
        result = db.trafico.update_one(
            {"id_tramo": document["id_tramo"]},
            {"$push": {"valores": document["valores"][0]}},
            upsert=True
        )
        if result.upserted_id:
            logging.info(f"A new document has been created with id_tramo: {document['id_tramo']}")
        else:
            logging.info(f"Updated document with id_tramo: {document['id_tramo']}")
    except Exception as e:
        logging.error(f"ERROR in upsert_document for {document['id_tramo']}: {e}")

def extractdata():
    """Extract data from files and process them."""
    path = "/home/vagrant/Documents/bigdata/data"
    df_trafic_path = os.path.join(path, 'trafico')

    if not os.listdir(df_trafic_path):
        logging.error("ERROR: No traffic data files in folder available")
        return

    logging.info(f'Number of files in directory: {len(os.listdir(df_trafic_path))}')

    for file_name in os.listdir(df_trafic_path):
        trafic_file_path = os.path.join(df_trafic_path, file_name)
        logging.info(f"Processing file: {trafic_file_path}")
        process_file(trafic_file_path)

# Run the function
extractdata()
