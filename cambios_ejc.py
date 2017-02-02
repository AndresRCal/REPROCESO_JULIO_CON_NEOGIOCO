# -*- coding: utf-8 -*-
"""
Created on Wed Feb  1 15:07:55 2017

@author: edejc
"""
import time
import pandas as pd
import os
import preproceso as process

intens1 = None

def importa_intensificadores():
    start=time.strftime("%H:%M:%S")
    path='user\\chunker\\'
    nfile='intensifiers.dat'
    intens=pd.read_csv(path+os.sep+nfile,sep='\t',encoding='latin_1',header=None,names=['palabra','valor'])
    end=time.strftime("%H:%M:%S")
    print(' importa_intensificadores()','Inicio:  ',start,'Fin:  ',end)
    return(intens)
    
def recorre_intensificadores(intens,post,event_id):
    ids=[]
    pals_sent=[]
    sent=[]
    posIni=[]
    posFin=[]
    long=[]
    for i in intens.index:
        if ' '+intens['palabra'][i].upper()+' ' in post.upper():
            ids+=[event_id]
            pals_sent+=[intens['palabra'][i]]
            sent+=[intens['valor'][i]]
            try:
                posIni+=[post.upper().index(intens['palabra'][i].upper())]
                posFin+=[post.upper().index(intens['palabra'][i].upper())+len(intens['palabra'][i].upper())]
                long+=[len(intens['palabra'][i].upper())]
            except Exception as msg:
                print(msg)
                posIni+=[False]
                posFin+=[False]
                long+=[False]
        elif ' '+intens['palabra'][i].upper()+'S ' in post.upper():
            ids+=[event_id]
            pals_sent+=[intens['palabra'][i]]
            sent+=[intens['valor'][i]]
            try:
                posIni+=[post.upper().index(intens['palabra'][i].upper())]
                posFin+=[post.upper().index(intens['palabra'][i].upper())+len(intens['palabra'][i].upper())]
                long+=[len(intens['palabra'][i].upper())]
            except Exception as msg:
                print(msg)
                posIni+=[False]
                posFin+=[False]
                long+=[False]
        elif ' '+intens['palabra'][i].upper()+'S ' in post.upper():
            ids+=[event_id]
            pals_sent+=[intens['palabra'][i]]
            sent+=[intens['valor'][i]]
            try:
                posIni+=[post.upper().index(intens['palabra'][i].upper())]
                posFin+=[post.upper().index(intens['palabra'][i].upper())+len(intens['palabra'][i].upper())]
                long+=[len(intens['palabra'][i].upper())]
            except Exception as msg:
                print(msg)
                posIni+=[False]
                posFin+=[False]
                long+=[False]
        else:
             pass
             
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
    
def DataFrame_pals_intens(df_pals_mod,intensificadores):
    start=time.strftime("%H:%M:%S")
    DaFr=pd.DataFrame()
    for pr in df_pals_mod.index:
        if pr == 0:
            DaFr=recorre_intensificadores(intensificadores,df_pals_mod['MSG'][pr],df_pals_mod['id'][pr])
        else:
            DaFr=DaFr.append(recorre_intensificadores(intensificadores,df_pals_mod['MSG'][pr],df_pals_mod['id'][pr]))
    end=time.strftime("%H:%M:%S")
    print(' DataFrame_pals_sentimiento()','Inicio:  ',start,'Fin:  ',end)
    return(DaFr)
    


if __name__ == '__main__':
   DF_Ref,DF_Reproc,M1,M2,df_pals_mod=process.reproceso_delimitacion()
   intens1=importa_intensificadores()
   DaFr=DataFrame_pals_intens(df_pals_mod,intens1)
   print(DaFr)

