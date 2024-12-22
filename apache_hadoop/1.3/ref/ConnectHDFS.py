#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 6 09:40:02 2023

@author: angel
"""
import subprocess

def run_cmd(args_list):
        """
        run linux commands
        """
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        s_output, s_err = proc.communicate()
        return s_output, s_err

##Run Hadoop -ls / command in Python
(out, err)= run_cmd(['hdfs', 'dfs', '-ls', '/'])
print(out)

##Run Hadoop -mkdir command in Python
(out, err)= run_cmd(['hdfs', 'dfs', '-mkdir', '/input2'])

##Run Hadoop -ls / command in Python
(out, err)= run_cmd(['hdfs', 'dfs', '-ls', '/'])
print(out)

##Run Hadoop -put command in Python
(out, err)= run_cmd(['hdfs', 'dfs', '-put', 
                          '/home/angel/Documents/BigData/mapreduce/WordFrequency/Book.txt', '/input2'])
                    
##Run Hadoop -get command in Python
(out, err)= run_cmd(['hdfs', 'dfs', '-get',
                          '/input2/Book.txt', '/home/angel/Documents/BigData/hadoop/'])
