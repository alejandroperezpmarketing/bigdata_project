import os
import re
from datetime import datetime, time
from pymongo import MongoClient as mc
from zoneinfo import ZoneInfo
from pymongo import UpdateOne

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
    """Check if the line contains a date in specified formats."""
    for i, pattern in enumerate(patterns):
        if re.search(pattern, line):
            return i
    return None

def find_coordinates_partial_match(estacion):
    # Split the station name into words
    estacion_words = estacion.split()
    
    # Iterate through the coordinates dictionary to find partial matches
    for station_name, coords in coordinates.items():
        # Check if any word in `estacion_words` is present in the `station_name`
        if any(word in station_name for word in estacion_words):
            return coords
    
    # Return None if no match is found
    return None


def extractdata_meteo():
    path = "/home/vagrant/Documents/bigdata/data/test"
    meteo_folder_name = 'meteo'
    df_meteo_path = os.path.join(path, meteo_folder_name)
    madrid_tz = ZoneInfo("Europe/Madrid")

    # Check if directory contains files
    files = os.listdir(df_meteo_path)
    if not files:
        print("ERROR: No traffic data files in folder available")
        return  # Exit if no files found

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
                    #latitude, longitude = None, None  # Initialize `latitude` and `longitude` to None
                    for line in lecture:
                        lect_line = line.replace('\n', '').strip().split("\t")

                        if "Estaci" in lect_line[0] and not estacion:
                            parts = line.strip().split("-", 1)
                            estacion = parts[-1].strip().replace('.', '')

                            if estacion.startswith(' '):
                                estacion = estacion[1:]

                            print(estacion)
                            station_coordinates = find_coordinates_partial_match(estacion)
                            if station_coordinates:
                                print(f"Coordinates found for '{estacion}': {station_coordinates}")
                            else:
                                print(f"No coordinates found for '{estacion}'")
                            if station_coordinates:
                                latitude, longitude = station_coordinates
                            else:
                                print(f"Coordinates not found for {estacion}")
                                continue

                        if "FECHA" in lect_line and "NO2" in lect_line:
                            pos_date = lect_line.index("FECHA")
                            pos_hour = lect_line.index("HORA")
                            pos_NO2 = lect_line.index("NO2")
                            print(f'Position of NO2: {pos_NO2}')
                            continue

                        if pos_date is not None:
                            date_format_index = contains_date(lect_line[pos_date])
                            if date_format_index is not None:
                                date_value = lect_line[pos_date].replace(' ', '')

                                # Match date format based on the identified pattern
                                try:
                                    if date_format_index == 0:  # YYYY-MM-DD
                                        date_obj = datetime.strptime(date_value, "%Y-%m-%d")        
                                    elif date_format_index == 1:  # DD/MM/YYYY or MM/DD/YYYY
                                        date_obj = datetime.strptime(date_value, "%d/%m/%Y")  # Adjust format as needed
                                    elif date_format_index == 2:  # Month Day, Year
                                        date_obj = datetime.strptime(date_value, "%B %d, %Y")

                                    else:
                                        print("ERROR: no date format in date")
                                    

                                    # Check if the month is May (05)
                                    if date_obj.month == 5:
                                        print(f'Document {i}, estacion: {estacion}, {station_coordinates} valid date: {date_obj.strftime("%Y-%m-%d")}')
                                    else:
                                        continue
                                    
                                    if pos_hour is not None:
                                        if lect_line[pos_hour]:
                                                # Convert the hour to a `time` object
                                                hour = int(lect_line[pos_hour])
                                                time_obj = time(hour, 0)  # Assuming minute and second are 0
                                                # Combine date and time
                                                combined_datetime = datetime.combine(date_obj, time_obj)   
                                                #Set Time zone to Madrid
                                                combined_datetime_madrid_timezone = combined_datetime.replace(tzinfo=madrid_tz)
                                                # Convert to ISO 8601 format
                                                date_iso_format = combined_datetime_madrid_timezone.isoformat()
                                                print(f'Combined DateTime in ISO format: {date_iso_format}')
                                        else:
                                            continue
                                    else:
                                        print(f"ERROR parsing Hour from {meteo_file_path} at line {id}")
                                        continue
                                    
                                    if pos_NO2 is not None and date_iso_format is not None and hour is not None:
                                        if  lect_line[pos_NO2]:
                                            if pos_NO2 is not None:
                                                NO2_value = int(lect_line[pos_NO2]) if lect_line[pos_NO2] else None
                                                if NO2_value is None:
                                                    continue
                                                    
                                                ####MONGODB
                                                # Dictionary definition
                                                    
                                                dictionary = [{"fecha": date_iso_format, "hora": hour, "NO2": NO2_value}]                       
                                                result = db.meteo.update_one({"id_estacion":id_estacion},{"$push":{"valores":{"fecha":date_value,"hora":hour, "NO2":NO2_value}}})
                                                if result.raw_result["nModified"] == 0 and result.modified_count == 0 and result.upserted_id == None:
                                                    db.meteo.insert_one({"id_estacion":id_estacion,
                                                                            "nombre":estacion,
                                                                            "coordenadas":[latitude,longitude],                                                                                                                                                  
                                                                            "valores":dictionary})
                                                    id_estacion += 1
                                                                        
                                                    print(f"A new document has been created with id_estacion: {id_estacion}, and name {estacion}")
                                                else:
                                                    print(f"New entry for the document with id_estacion and name: {id_estacion}-{estacion}")
                                                                                    
                                    else:
                                        print(f"ERROR parsing NO2 value from {meteo_file_path} at line {id}")
                                        continue
                                        
                                except Exception as e:
                                    print(f'Error parsing date in file {file_name} line "{line}": {e}')
                            
                            
                                                
                            else:
                                print(f"ERROR parsing date from {meteo_file_path} at line {id}")
                                continue
                                                      
            except Exception as e:
                print(f'Error processing file {file_name}: {e}')

extractdata_meteo()
