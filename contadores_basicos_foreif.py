# -*- coding: utf-8 -*-
"""
Created on Fri Feb 10 10:46:19 2017

@author: AAREYESC
"""
import pandas as pd
import cambios_ejc as ejc
import limpieza_posteos as limp
import os
import alGusto as alg

def M1(DF_Ref,DF_Reproc):
    ext1=DF_Ref[['event_id_no','Mensaje','Sentimiento']]
    ext2=DF_Reproc[['event_id_no','Sentimiento']]
    M1=pd.merge(ext1,ext2,on='event_id_no',how='inner')
    M1Mensaje=[]
    M1Mensaje_sucio=[]
    for l in M1.index:
        basura,auxiliar=limp.replacing_substrings(M1['Mensaje'][l])
        seq=['Posteo',str(M1['event_id_no'][l]),'Limpieza',str(basura)]
        text_to_log=ejc.concat_espacesep(seq)
#        print(text_to_log)
        try:
            ejc.escribe_log(text_to_log)
        except Exception as msg:
            ejc.escribe_log(ejc.concat_espacesep(str([M1['event_id_no'][l],msg])))
            print(ejc.concat_espacesep([M1['event_id_no'][l],msg]))
        M1Mensaje+=[auxiliar.strip(' ').strip('\n').strip('\t')]
        M1Mensaje_sucio+=[M1['Mensaje'][l]]
    M1['Mensaje_original']=M1Mensaje_sucio
    M1['Mensaje']=M1Mensaje
    return(M1)

DF_Ref=pd.read_csv('''D:\\endeca\\ReprocesoSentimiento\\Pues_va_de_new\\Pruebas_con_negocio\\REPROCESO_JULIO_CON_NEOGIOCO'''+os.sep+'GaleriasRS_Referencia_limpio.csv',encoding='utf8',sep='\t')
DF_Reproc=pd.read_csv('''D:\\endeca\\ReprocesoSentimiento\\Pues_va_de_new\\Pruebas_con_negocio\\REPROCESO_JULIO_CON_NEOGIOCO'''+os.sep+'GaleriasRS_limpio.csv',encoding='utf8',sep='\t')
registros_referencia=max(DF_Ref.index)+1
registros_reproceso=max(DF_Reproc.index)+1
M1=M1(DF_Ref,DF_Reproc)                       
alg.Dframe_to_sqlitehome(os.getcwd(),alg.dar_nombre('BaseDatos','reproceso','sqlite'),'WORK_Layout'+str(alg.fecha).replace('/',''),'w',M1)
pals_mod=['hermosa','hermosamente','hermosura','divercion','diverción','diversion','diversión','divertida','divertidas','divertido','divertidos','divertimos','divertir','favorita','favoritas','favorito','favoritos','exito','éxito','exitos','exitosa','exitosas','exitoso','exitosos','feliz']
