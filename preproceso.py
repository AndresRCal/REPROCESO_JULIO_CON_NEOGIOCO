# -*- coding: utf-8 -*-
"""
Created on Tue Jan 24 16:27:49 2017

@author: AAREYESC
"""
import time
import pandas as pd
import os


def recorre_general(general,post,event_id):
    ids=[]
    pals_sent=[]
    sent=[]
    posIni=[]
    posFin=[]
    long=[]
    for i in general.index:
        if general['palabra'][i].upper() in post.upper():
            ids+=[event_id]
            pals_sent+=[general['palabra'][i]]
            sent+=[general['sentimiento'][i]]
            try:
                posIni+=[post.upper().index(general['palabra'][i].upper())]
                posFin+=[post.upper().index(general['palabra'][i].upper())+len(general['palabra'][i].upper())]
                long+=[len(general['palabra'][i].upper())]
            except Exception as msg:
                print(msg)
                posIni+=[False]
                posFin+=[False]
                long+=[False]
    dica={
            'ids':ids,
            'pals_sent':pals_sent,
            'sent':sent,
            'long':long,
            'posIni':posIni,
            'posFin':posFin
          }
    DaFa=pd.DataFrame(dica)
    return(DaFa[['ids','pals_sent','sent','posIni','posFin','long']])

def reproceso_delimitacion():
    start=time.strftime("%H:%M:%S")
#    IMPORTA DATAFRAME LIMPIO DE GALERIAS REFERENCIA
    DF_Ref=pd.read_csv('''D:\\endeca\\ReprocesoSentimiento\\Pues_va_de_new\\Pruebas_con_negocio\\REPROCESO_JULIO_CON_NEOGIOCO'''+os.sep+'GaleriasRS_Referencia_limpio.csv',encoding='utf8',sep='\t')
#    IMPORTA DATAFRAME LIMPIO DE GALERIAS REPROCESADO
    DF_Reproc=pd.read_csv('''D:\\endeca\\ReprocesoSentimiento\\Pues_va_de_new\\Pruebas_con_negocio\\REPROCESO_JULIO_CON_NEOGIOCO'''+os.sep+'GaleriasRS_limpio.csv',encoding='utf8',sep='\t')
    ext1=DF_Ref[['event_id_no','Mensaje','Sentimiento']]
    ext2=DF_Reproc[['event_id_no','Sentimiento']]
    M1=pd.merge(ext1,ext2,on='event_id_no',how='inner')
    M2=M1[M1['Sentimiento_x']!=M1['Sentimiento_y']]
    id=[]
    MSG=[]
    Sent1=[]
    Sent2=[]
#    SE CREA EL DATAFRAME CON LAS FRASES QUE TIENEN LAS PALABRAS MODIFICADAS EN EL DICCIONARIO1
    pals_mod=['hermosa','hermosamente','hermosura','divercion','diverción','diversion','diversión','divertida','divertidas','divertido','divertidos','divertimos','divertir','favorita','favoritas','favorito','favoritos','exito','éxito','exitos','exitosa','exitosas','exitoso','exitosos','feliz']
    for k in M2.index:
        cnter=0
        for i in pals_mod:
            if cnter <= 0:
                if i in M2['Mensaje'][k]:
                    id+=[M2['event_id_no'][k]]
                    MSG+=[M2['Mensaje'][k]]
                    Sent1+=[M2['Sentimiento_x'][k]]
                    Sent2+=[M2['Sentimiento_y'][k]]
                    cnter=cnter+1
                else:
                    continue
    dict_pals_mod={
                    'id':id,
                    'MSG':MSG,
                    'Sent1':Sent1,
                    'Sent2':Sent2
                   }
    df_pals_mod=pd.DataFrame(dict_pals_mod).drop_duplicates()
    end=time.strftime("%H:%M:%S")
    print(' reproceso_delimitacion()','Inicio:  ',start,'Fin:  ',end)
    return(DF_Ref,DF_Reproc,M1,M2,df_pals_mod)

def importa_General():
    start=time.strftime("%H:%M:%S")
    path='''C:\\Users\\AAREYESC\\Documents\\GitHub\\analisis-sentimiento\\user\\salience\\sentiment'''
    nfile='general.hsd'
    General=pd.read_csv(path+os.sep+nfile,sep='\t',encoding='latin_1',header=None,names=['palabra','sentimiento'])
    end=time.strftime("%H:%M:%S")
    print(' importa_General()','Inicio:  ',start,'Fin:  ',end)
    return(General)
    
def DataFrame_pals_sentimiento(df_pals_mod,General):
    start=time.strftime("%H:%M:%S")
    DaFr=pd.DataFrame()
    for pr in df_pals_mod.index:
        if pr == 0:
            DaFr=recorre_general(General,df_pals_mod['MSG'][pr],df_pals_mod['id'][pr])
        else:
            DaFr=DaFr.append(recorre_general(General,df_pals_mod['MSG'][pr],df_pals_mod['id'][pr]))
    end=time.strftime("%H:%M:%S")
    print(' DataFrame_pals_sentimiento()','Inicio:  ',start,'Fin:  ',end)
    return(DaFr)

def multiplicador_adm(post,event_id):
    cnt=0
    indices=[]
    pos_barrido=0
    for i in post:
        if '!' == i:
            cnt=cnt+1
    if cnt == 0:
        dic_adm={
                 'event_id':[event_id],
                 'Signo':['!'],
                 'Cantidad_adm':['0'],
                 'Posiciones_adm':['0']
                 }
#        return(event_id,'!','0','0')
        return(pd.DataFrame(dic_adm))
    else:
        while len(indices) < cnt:
            if post[pos_barrido]=='!':
                indices+=[pos_barrido]
                pos_barrido=pos_barrido+1
            else:
                pos_barrido=pos_barrido+1
        dic_adm={
                 'event_id':[event_id],
                 'Signo':['!'],
                 'Cantidad_adm':[cnt],
                 'Posiciones_adm':[indices]
                 }
#        return(event_id,'!',cnt,indices)
        return(pd.DataFrame(dic_adm))

def dat_finding(post,event_id):
    cnt=0
    indices=[]
    pos_barrido=0
    for i in post:
        if '.' == i:
            cnt=cnt+1
    if cnt == 0:
        dic_adm={
                 'event_id':[event_id],
                 'Signo':['.'],
                 'Cantidad_points':['0'],
                 'Posiciones_point':['0']
                 }
        return(pd.DataFrame(dic_adm))
    else:
        while len(indices) < cnt:
            if post[pos_barrido]=='.':
                indices+=[pos_barrido]
                pos_barrido=pos_barrido+1
            else:
                pos_barrido=pos_barrido+1
        dic_adm={
                 'event_id':[event_id],
                 'Signo':['.'],
                 'Cantidad_points':[cnt],
                 'Posiciones_point':[indices]
                 }
        return(pd.DataFrame(dic_adm))
        
def Build_DaFr_symbols(df_pals_mod):
    start=time.strftime("%H:%M:%S")
    adm=pd.DataFrame()
    point=pd.DataFrame()
    for i in df_pals_mod.index:
        if i == 0:
            adm=multiplicador_adm(df_pals_mod['MSG'][i],df_pals_mod['id'][i])
            point=dat_finding(df_pals_mod['MSG'][i],df_pals_mod['id'][i])
        else:
            adm=adm.append(multiplicador_adm(df_pals_mod['MSG'][i],df_pals_mod['id'][i])) 
            point=point.append(dat_finding(df_pals_mod['MSG'][i],df_pals_mod['id'][i]))
    end=time.strftime("%H:%M:%S")
    print('Build_DaFr_symbols()','Inicio:  ',start,'Fin:  ',end)
    merg=pd.merge(point,adm,on='event_id',how='outer')
    return(merg)
        
#DF_Ref,DF_Reproc,M1,M2,df_pals_mod=reproceso_delimitacion()
#General=importa_General()
#DaFr=DataFrame_pals_sentimiento(df_pals_mod,General)
#DaFr_symbols=Build_DaFr_symbols(df_pals_mod)
#print('Gracias.. hasta luego!')



















