#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 18 19:08:33 2018

@author: angelmartinfurones
"""
#CLASE QUE SIMULA UNA CALCULADORA
class Calculadora(object):
    #EL CONSTRUCTOR INICIA "VALOR" A 0
    def __init__(self): #SELF HACE REFERENCIA A SI MISMO
        self.valor=0
    #SUMA UN NUMERO 'N' AL VALOR
    def suma(self,n):
        self.valor +=n
    def getValor(self):
        return self.valor

calc = Calculadora() #INSTANCIA UN OBJETO EN LA VARIABLE CALC

calc.suma(2) #SUMA 2 AL VALOR DE LA CALCULADORA

print(calc.getValor()) #MOSTRARIA UN DOS EN LA SHELL

calc.suma(2) #SE SUMA 2 A LA VARIABLE VALOR

print(calc.getValor())
