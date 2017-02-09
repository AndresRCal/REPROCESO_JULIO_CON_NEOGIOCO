# -*- coding: utf-8 -*-
"""
Created on Thu Feb  2 11:55:34 2017

@author: edejc
"""

import preproceso as process
import pandas as pd

pals_mod=['divercion','diverción','diversion','diversión','divertida','divertidas','divertido','divertidos','divertimos','divertir','favorita','favoritas','favorito','favoritos','exito','éxito','exitos','exitosa','exitosas','exitoso','exitosos','feliz']
DF_Ref,DF_Reproc,M1,M2,df_pals_mod=process.reproceso_delimitacion()

def contadores():
    pal_mod_Global = encuentra_palabras(DF_Ref,1)
    pal_mod = encuentra_palabras(DF_Reproc,1)
    auxc = encuentra_palabras(M2,2)
    auxc1 = encuentra_palabras_esp(df_pals_mod)
    conteo_pal= pal_mod_Global['id'].count()
    conteo_pal1= pal_mod['id'].count()
    conteo_M2 = M2['event_id_no'].count()
    conte3= auxc['id'].count()
    conte4 = auxc1['id'].count()
    print('***********************************************************')
    print('Palabras modificadas encontradas en Reproceso '+str(conteo_pal))
    print('Palabras modificadas encontradas en Referencia '+str(conteo_pal1))
    print('Frases en M2: '+str(conte3))
    print('Palabras mod en df_pals_mod: '+str(conte4))
    return(pal_mod_Global,pal_mod,auxc,auxc1)
    
    
def encuentra_palabras(df,t):
    ids=[]
    MSG=[]
    Sent1=[]
    for k in df.index:
        flg=0
        for i in pals_mod:
            if flg <= 0:
                if ' '+i+' ' in df['Mensaje'][k]:
                    ids+=[df['event_id_no'][k]]
                    MSG+=[df['Mensaje'][k]]
                    if(t==1):
                        Sent1+=[df['Sentimiento'][k]]
                    else:
                        Sent1+=[df['Sentimiento_y'][k]]
                    
                    flg+=1
                else:
                    continue
                
    dict_pals_mod={
                    'id':ids,
                    'MSG':MSG,
                    'Sent1':Sent1
                  }
    salida=pd.DataFrame(dict_pals_mod).drop_duplicates()
#    print(conteo_pal)
    return(salida)
    
def encuentra_palabras_esp(df):
    ids=[]
    MSG=[]
    Sent1=[]
    for k in df.index:
        flg=0
        for i in pals_mod:
            if flg <= 0:
                if ' '+i+' ' in df['MSG'][k]:
                   ids+=[df['id'][k]]
                   MSG+=[df['MSG'][k]]
                   Sent1+=[df['Sent2'][k]]
                    
                   flg+=1
                else:
                    continue
                
    dict_pals_mod={
                    'id':ids,
                    'MSG':MSG,
                    'Sent1':Sent1
                  }
    salida=pd.DataFrame(dict_pals_mod).drop_duplicates()
#    print(conteo_pal)
    return(salida)                      
   
    
pal_mod_Global,pal_mod,auxc,auxc1=contadores() 
    
