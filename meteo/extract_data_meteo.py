# Author: ALEJANDRO PEREZ PEÑALVER
# This document allow you to connect to a mongo 4.4.29 database in a docker container to
# import and update documents on a collections
# Project URL: https://github.com/alejandroperezpmarketing/bigdata_project
# MongoDB Docker container URL: https://github.com/alejandroperezpmarketing/mogodb
# The update_one() method followed by insert_one() is fine for relatively small datasets, 
# bulk_write() or another batch operation with bid databases to optimize MongoDB interactions.

import pandas as pd
import os
import json
import pymongo as pmo
from pymongo import MongoClient as mc


coordinates = {
"Avda. Francia": [39.457504, -0.342689],
"Bulevard Sud": [39.450378, -0.396313],
"Molí del Sol": [39.481138, -0.408558],
"Pista de Silla": [39.458060, -0.376653],
"Politècnic": [39.479621, -0.337407],
"Centre": [39.470718, -0.376384],
"Vivers": [39.479488, -0.369550],
"Port Moll Trans. Ponent": [39.459264, -0.323217],
"Nazaret Met-2": [39.448554, -0.333289],
"Conselleria Meteo": [39.472034, -0.404878],
"Olivereta": [39.469238, -0.406037],
"Port Turia": [39.450518, -0.328945]}


client = mc('mongodb://vagrant:vagrant@localhost:27017/bigdata?authSource=admin')

db = client.bigdata




def extractdata_meteo():
    path="/home/vagrant/Documents/bigdata_project/meteo"
    #path = "/home/vagrant/Documents/bigdata/data"
    meteo_folder_name = 'estaciones_metereologicos'
    df_meteo_path = os.path.join(path, meteo_folder_name)
    
    
    #########################
    # Check if the traffic directory contains any files
    if not os.listdir(df_meteo_path):
        print("ERROR: No traffic data files in folder available")
        return  # Exit the function if no files are found
    else:
        print("ok meteo")
        count_meteo = len(os.listdir(df_meteo_path))  # Get number of files
        print(f'Number of files in directory: {count_meteo}')
        
        if count_meteo == 12:
            print("ok: 12 meteo stations in this directory")
        else:
            print("ERROR: not enouth files to start working")
            
            
extractdata_meteo()

    
    

