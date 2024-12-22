#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Created on Mon Aug 19 08:58:29 2024
@author: angelmartinfurones
"""
from mrjob.job import MRJob
from mrjob.step import MRStep

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
            yield int(punto),int(intensidad)
            
    def reducer_init(self):
        self.Names = {}
        file1=open("/Users/angelmartinfurones/Documents/master/big_data/datos/TRA_ESPIRAS_P-2.CSV")
        while True:
            line=file1.readline().decode('ascii','ignore')
            if not line:break
            fields=line.split(';')
            if str(fields[0]) != 'X' and fields[5] != None:
                coord=str(str(fields[0])+","+str(fields[1]))
                clave=str(fields[5])
                self.Names.update({clave:coord})
                
    def reducer_count_ratings(self,punto,intensidad):
        total = 0
        numElements = 0
        for x in intensidad:
            total += x
            numElements += 1
        try:
            yield self.Names[str(punto)], total / numElements
        except:
            yield str(punto), total / numElements
    
if __name__ == '__main__':
    IntensidadPuntos.run()
