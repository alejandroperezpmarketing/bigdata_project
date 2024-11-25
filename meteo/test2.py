import os
import re
from datetime import datetime, time
from pymongo import MongoClient as mc
from zoneinfo import ZoneInfo

# Define date patterns to match different date formats
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
    estacion_words = estacion.split()
    for station_name, coords in coordinates.items():
        if any(word in station_name for word in estacion_words):
            return coords
    return None

def extractdata_meteo():
    path = "/home/vagrant/Documents/bigdata/data/test"
    meteo_folder_name = 'meteo'
    df_meteo_path = os.path.join(path, meteo_folder_name)
    madrid_tz = ZoneInfo("Europe/Madrid")

    files = os.listdir(df_meteo_path)
    if not files:
        print("ERROR: No traffic data files in folder available")
        return

    print("ok meteo")
    count_meteo = len(files)
    print(f'Number of files in directory: {count_meteo}')

    if count_meteo == 2:
        print("ok: 2 meteo stations in this directory")
        id_estacion = 1
        
        for i, file_name in enumerate(files):
            meteo_file_path = os.path.join(df_meteo_path, file_name)
            print(f"Opening file: {meteo_file_path}")

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
                            print(estacion)

                            station_coordinates = find_coordinates_partial_match(estacion)
                            if not station_coordinates:
                                print(f"No coordinates found for '{estacion}', skipping file.")
                                continue
                            latitude, longitude = station_coordinates

                        if "FECHA" in lect_line and "NO2" in lect_line:
                            pos_date = lect_line.index("FECHA")
                            pos_hour = lect_line.index("HORA")
                            pos_NO2 = lect_line.index("NO2")
                            continue

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
                                    #date_iso_format = combined_datetime.replace(tzinfo=madrid_tz).isoformat()

                                    NO2_value = int(lect_line[pos_NO2]) if lect_line[pos_NO2] else None
                                    if NO2_value is None:
                                        continue

                                    # MongoDB Update Operation
                                    # MongoDB Update Operation
                                    result = db.meteo.update_one(
                                        {"id_estacion": id_estacion},  # Filter by id_estacion only
                                        {
                                            "$push": {
                                                "valores": {
                                                    "fecha": combined_datetime.replace(tzinfo=madrid_tz),  # Store as ISODate # Use datetime object, not string
                                                    "hora": hour,
                                                    "NO2": NO2_value
                                                }
                                            },
                                            "$setOnInsert": {  # Sets these fields only if a new document is created
                                                "nombre": estacion,
                                                "coordenadas": [latitude, longitude]
                                            }
                                        },
                                        upsert=True  # Insert if not found
                                    )
                                    
                                    if result.upserted_id:
                                        print(f"A new document created with id_estacion: {id_estacion}, name: {estacion}")

                                    else:
                                        print(f"Added new entry for existing document id_estacion: {id_estacion}, name: {estacion}")

                                except Exception as e:
                                    print(f'Error parsing date in file {file_name} line "{line}": {e}')
                            else:
                                print(f"ERROR parsing date from {meteo_file_path}")
            except Exception as e:
                print(f'Error processing file {file_name}: {e}')

extractdata_meteo()


document = db.meteo.find_one({"id_estacion": 2})
print(document)