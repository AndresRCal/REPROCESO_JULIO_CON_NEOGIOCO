# -*- coding: utf-8 -*-
"""
Created on Thu Feb  9 10:02:20 2017

@author: AAREYESC
"""
import pandas as pd
import time
import cambios_ejc as ejc

def ranges_symbols(event_id,pos_adms,pos_point,cnt_adms,cnt_points):
    result=[]
    pos_inis=[]
    pos_fins=[]
    if cnt_adms == '0':
        return(event_id,0,0)
    elif cnt_adms != '0' and cnt_points == '0':
        return(event_id,0,max(pos_adms))
    else:
        for i in pos_adms:
            pos_aux=0
            for j in pos_point:
                if pos_aux==0 and int(j) < int(i):
                    pos_aux=j
                else:
                    pass
            pos_inis+=[pos_aux]
            pos_fins+=[i]
        result=[event_id,pos_inis,pos_fins]
        return(result[0],result[1],result[2])

def finding_ranges_symbols(DaFr_symbols):
    start=time.strftime("%H:%M:%S")
    ids=[]
    rango_symbols_ini=[]
    rango_symbols_fin=[]
    for i in DaFr_symbols.index:
        a,b,c=ranges_symbols(DaFr_symbols['event_id'][i],DaFr_symbols['Posiciones_adm'][i],DaFr_symbols['Posiciones_point'][i],DaFr_symbols['Cantidad_adm'][i],DaFr_symbols['Cantidad_points'][i])
        ids+=[a]
        rango_symbols_ini+=[b]
        rango_symbols_fin+=[c]
    df_aux={
            'ids':ids,
            'rango_symbols_ini':rango_symbols_ini,
            'rango_symbols_fin':rango_symbols_fin
            }
    df_aux=pd.DataFrame(df_aux)
    DaFr_symbols=pd.merge(DaFr_symbols,df_aux,left_on='event_id',right_on='ids',how='left')
    end=time.strftime("%H:%M:%S")
    print(' finding_ranges_symbols()','Inicio:  ',start,'Fin:  ',end)
    seq=['finding_ranges_symbols()','Inicio:',start,'Fin:',end]
    text_to_log=ejc.concat_espacesep(seq)
    ejc.escribe_log(text_to_log)
    return(DaFr_symbols[['event_id','Signo_x','Cantidad_points','Posiciones_point','Signo_y','Cantidad_adm','Posiciones_adm','rango_symbols_ini','rango_symbols_fin']])

def M_Precalculo(DaFr_sent,DaFr_symbols,DaFr_Intens):
    MPrecalculo=pd.merge(DaFr_sent,DaFr_symbols,left_on='ids',right_on='event_id',how='left')
    MPrecalculo=pd.merge(MPrecalculo,DaFr_Intens,on='ids',how='left')
    MPrecalculo=MPrecalculo[['ids', 'pals_sent', 'sent', 'posIni', 'posFin','rango_symbols_ini','rango_symbols_fin',
        'pals_intens', 'valor_intens', 'posIni_intens','posFin_intens']]
    return(MPrecalculo.fillna(0))