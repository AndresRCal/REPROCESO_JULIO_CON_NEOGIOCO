# -*- coding: utf-8 -*-
"""
Created on Wed Feb  1 15:07:55 2017

@author: edejc
"""
import time
import pandas as pd
import os

def importa_intensificadores():
    start=time.strftime("%H:%M:%S")
    path='user\\chunker\\'
    nfile='intensifiers.dat'
    intens=pd.read_csv(path+os.sep+nfile,sep='\t',encoding='latin_1',header=None,names=['palabra','valor'])
    end=time.strftime("%H:%M:%S")
    print(' importa_intensificadores()','Inicio:  ',start,'Fin:  ',end)
    return(intens)
    
