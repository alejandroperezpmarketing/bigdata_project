import pandas as pd
import os
import json
import pymongo as pmo


def extractdata():
    # Set the paths for your data directories
    path = "/home/vagrant/Documents/bigdata/bigdata_project/data"
    trafic_folder_name = 'trafico'
    meteo_folder_name = 'estaciones_metereologicos'
    df_trafic_path = os.path.join(path, trafic_folder_name)
    df_meteo_path = os.path.join(path, meteo_folder_name)

    grouped_data = {}  # Dictionary to store grouped data


    #########################
    ######MONGODB CONEXTION#############################
    
    db = "db"
    
    #########################
    # Check if the traffic directory contains any files
    if not os.listdir(df_trafic_path):
        print("ERROR: No traffic data files in folder available")
        return  # Exit the function if no files are found
    else:
        print("ok trafic")
        count_trafic = len(os.listdir(df_trafic_path))  # Get number of files
        print(f'Number of files in directory: {count_trafic}')

        # Loop through each file in the traffic directory
        for i, y in enumerate(os.listdir(df_trafic_path)):
            try:
                trafic_file_path = os.path.join(df_trafic_path, y)
                print(f"Processing file: {trafic_file_path}")

                # Extract date parts from filename assuming it's in the format '2024-5-3T3H0m.csv'
                date_parts = y.split(".")[0]  # Strip file extension
                year, month, day_and_time = date_parts.split(
                    "-")[0], date_parts.split("-")[1], date_parts.split("-")[-1]

                with open(trafic_file_path, 'r', encoding="windows-1252") as file:

                    while True:
                        linea = file.readline()
                        if not linea:
                            break

                        lect_linea = linea.split(";")

                        try:
                            id_tramo = str(lect_linea[1])

                            # Split 'geo_point_2d' into latitude and longitude
                            latitude, longitude = str(lect_linea[-1]).split(",")
                            lectura = float(lect_linea[2])
                            direccion = str(lect_linea[3])
                            estado = float(lect_linea[4])

                            # Dictionary definition
                            if lectura == -1 or lectura > 5000 or estado != 0:
                                lectura = None
                                dictionary = [{"fecha": f"{year}-{month}-{day_and_time}",
                                               "lectura": lectura}]
                            else:
                                # Dictionary definition
                                dictionary = [{"fecha": f"{year}-{month}-{day_and_time}",
                                               "lectura": lectura}]
                                
                                #print(dictionary)
                            
                                
                            #########################
                            ####create document on MONGODB
                            
                            #update a documento alrady created and if this documento does not exist create it
                            result = db.update_one({"id_tramo":id_tramo},{"$PUSH":{"valores":{"fecha":date_parts},
                                                                                   "lectura":lectura}})
                            
                            if result.raw_result["nModified"] == 0:
                                
                                db.insert_one({"id_tramo":id_tramo,
                                               "direccion":direccion,
                                               "coordenadas":[latitude,longitude],
                                               "valores":dictionary})
                            
                        except Exception as e:
                            print(f"ERROR reading {trafic_file_path}: {e}")

                        
            except Exception as e:
                print(f"ERROR reading {trafic_file_path}: {e}")



# Run the function
extractdata()
