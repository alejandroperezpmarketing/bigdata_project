import pandas as pd
import os
import numpy as np
import json

def extractdata(source_encoding='utf-8', target_encoding='window-1252'):
    # 1. Define paths
    path = "/home/vagrant/Documents/bigdata/data"
    trafic_folder_name = 'trafico'
    meteo_folder_name = 'estaciones_metereologicos'
    meteo_path = f'{path}/{meteo_folder_name}'
    trafic_path = f'{path}/{trafic_folder_name}'

    # 1.1 Check if the directories have files
    if not os.listdir(meteo_path):
        print("ERROR: No meteo data files in folder available")
        return  # Or exit() if you prefer to stop execution

    else:
        print("Meteo data directory is OK")

    if not os.listdir(trafic_path):
        print("ERROR: No trafic data files in folder available")
        return  # Or exit()

    else:
        print("Trafic data directory is OK")
        count = np.size(os.listdir(trafic_path))
        print(f'Number of files in directory: {count}')

    # 2. Process each file in the directory
    for i, y in enumerate(os.listdir(trafic_path)):
        df_path = f'{trafic_path}/{y}'
        
        # Extract date components from file name
        y_no_extension = y.split(".")[0]
        date_parts = y_no_extension.split("-")
        
        if len(date_parts) >= 3:
            year, month, hour = date_parts[0], date_parts[1], date_parts[2]
        else:
            print(f"ERROR: Filename format incorrect for {y}")
            continue  # Skip this file

        # Check if the file is empty
        if os.path.getsize(df_path) == 0:
            print(f"ERROR: {df_path} is empty, skipping.")
            continue

        # Read CSV file with error handling for empty or malformed files
        try:
            df = pd.read_csv(df_path, delimiter=";", encoding=source_encoding)

            # Check if the DataFrame has columns
            if df.empty or df.shape[1] == 0:
                print(f"ERROR: No valid data in {df_path}, skipping.")
                continue

        except pd.errors.EmptyDataError:
            print(f"ERROR: {df_path} is empty or has no data to parse.")
            continue
        except pd.errors.ParserError:
            print(f"ERROR: {df_path} is not a valid CSV or has malformed data.")
            continue
        except Exception as e:
            print(f"ERROR reading {df_path}: {e}")
            continue

        # Document formatting
        document = {
            'date': {
                'date_cod': i,
                'compl_date': y_no_extension,
                'year': year,
                'month': month,
                'hour': hour
            },
            'data': df.to_dict(orient='records')  # Convert DataFrame to list of dicts
        }

        # Print or process the document
        print(json.dumps(document, indent=4))

        # You can remove `break` to process all files
        break

# Example usage
extractdata('utf-8', 'windows-1252')
