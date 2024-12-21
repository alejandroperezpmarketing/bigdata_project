import pandas as pd
import os
import json

def extractdata():
    # Set the paths for your data directories
    path = "/home/vagrant/Documents/bigdata/bigdata_project/data"
    trafic_folder_name = 'trafico'
    meteo_folder_name = 'estaciones_metereologicos'
    df_trafic_path = os.path.join(path, trafic_folder_name)
    df_meteo_path = os.path.join(path, meteo_folder_name)
    
    grouped_data = {}  # Dictionary to store grouped data

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
                year, month, day_and_time = date_parts.split("-")[0], date_parts.split("-")[1], date_parts.split("-")[-1]
                
                # Read the CSV file
                df = pd.read_csv(trafic_file_path, delimiter=';', encoding="windows-1252")
                
                # Split 'geo_point_2d' into latitude and longitude
                df[['latitude', 'longitude']] = df['geo_point_2d'].str.split(',', expand=True)
                
                # Group data by 'Id. Tram / ID. Tramo' and store it in the dictionary
                for unique_id, group in df.groupby('Id. Tram / ID. Tramo'):
                    if unique_id not in grouped_data:
                        # Append the extracted data
                        latitude = group['latitude'].iloc[0]  # Get the first latitude in the group
                        longitude = group['longitude'].iloc[0]  # Get the first longitude in the group
                        
                        grouped_data[unique_id] = {
                            "id_tram": unique_id,
                            "Direccion": group['Descripció / Descripción'].iloc[0],
                            "Valores": [],
                            "Coordinates": [longitude, latitude]
                        }

                    # Extract 'Lectura' for the given ID
                    lectura = group['Lectura'].iloc[0]  # Take the first 'Lectura' in the group
                    
                    # Append the new data into the 'Valores' list
                    grouped_data[unique_id]["Valores"].append({
                        "Fecha": f"{year}-{month}-{day_and_time}",  # Concatenate date parts
                        "Lectura": float(lectura)
                    })

                # Print grouped data in JSON format for inspection
                print(json.dumps(grouped_data, indent=4))
                
                # Process only one file for now (remove 'break' for all files)
                #break

            except Exception as e:
                print(f"ERROR reading {trafic_file_path}: {e}")
                
        return grouped_data

# Run the function
extractdata()
