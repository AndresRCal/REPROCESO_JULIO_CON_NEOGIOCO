# -*- coding: utf-8 -*-
"""
Created on Thu Feb  2 11:55:34 2017

@author: edejc
"""

import preproceso as process
import pandas as pd
conteo_pal=None

pals_mod=['hermosa','hermosamente','hermosura','divercion','diverción','diversion','diversión','divertida','divertidas','divertido','divertidos','divertimos','divertir','favorita','favoritas','favorito','favoritos','exito','éxito','exitos','exitosa','exitosas','exitoso','exitosos','feliz']
DF_Ref,DF_Reproc,M1,M2,df_pals_mod=process.reproceso_delimitacion()

def contadores():
    
    return()
    
    
def encuentra_palabras():
    ids=[]
    MSG=[]
    Sent1=[]
    for k in DF_Reproc.index:
        cnter=0
        for i in pals_mod:
            if cnter <= 0:
                if i in DF_Reproc['Mensaje'][k]:
                    ids+=[DF_Reproc['event_id_no'][k]]
                    MSG+=[DF_Reproc['Mensaje'][k]]
                    Sent1+=[DF_Reproc['Sentimiento'][k]]
                    cnter=cnter+1
                else:
                    continue
                
    dict_pals_mod={
                    'id':ids,
                    'MSG':MSG,
                    'Sent1':Sent1
                  }
    conteo_pal=pd.DataFrame(dict_pals_mod).drop_duplicates()
#    print(conteo_pal)
    return()
    
encuentra_palabras()