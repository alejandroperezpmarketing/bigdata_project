#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Created on Mon Aug 19 08:58:49 2024
@author: angelmartinfurones
"""
from mrjob.job import MRJob
from mrjob.step import MRStep
import utm

class IntensidadPuntos(MRJob):
    def steps(self):
        return [
                MRStep(mapper=self.mapper_get_ratings,
                       reducer_init=self.reducer_init,
                       reducer=self.reducer_count_ratings)
                ]
    def mapper_get_ratings(self,key,line):
        (punto,mes,dia,anyo,hora,intensidad) = line.split(',')
        if punto != 'Punto':
            yield str(punto),int(intensidad)
            
    def reducer_init(self):
        self.Names = {}
        file1=open("/home/angel/Documents/BigData/mapreduce/intensidad/TRA_ESPIRAS_P-2.CSV")
        while True:
            line=file1.readline()
            if not line:break
            fields=line.split(';')
            if str(fields[0]) != 'X' and fields[5] != None:
                lat,lon=utm.to_latlon(float(fields[0]), float(fields[1]), 30,'N')
                coord=str(str(lon)+","+str(lat))
                clave=str(fields[5])
                self.Names.update({clave:coord})
                
    def reducer_count_ratings(self,punto,intensidad):
        total = 0
        numElements = 0
        for x in intensidad:
            total += x
            numElements += 1
        try:
            yield self.Names[str(punto)], int(total / numElements)
        except:
            yield str(punto), int(total / numElements)
    
if __name__ == '__main__':
    IntensidadPuntos.run()
