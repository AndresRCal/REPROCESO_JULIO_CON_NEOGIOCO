# -*- coding: utf-8 -*-
"""
Created on Wed Feb  1 15:07:55 2017

@author: edejc
"""
import time
import pandas as pd
import os
#import preproceso as process
def fecha_actual():
    itoday = time.strftime("%d")
    fecha= time.strftime("%Y%m")
    fecha=str(fecha)+str(itoday)
    return(fecha)
    
def escribe_log(texto):
    var_fecha=fecha_actual()
    ruta_log = 'log'+str(var_fecha)+'.txt'
#    print(var_fecha)
    archi=open(ruta_log,'a')
    archi.write(texto)
    archi.write('\n')
    return()
    
intens = pd.DataFrame()
#DaFr_Intens=pd.DataFrame()

def importa_intensificadores():
    path='user\\chunker\\'
    nfile='intensifiers.dat'
    intens=pd.read_csv(path+os.sep+nfile,sep='\t',encoding='latin_1',header=None,names=['palabra','valor'])
    return(intens)
    
def recorre_intensificadores(intens,post,event_id):
    ids=[]
    pals_intens=[]
    valor=[]
    posIni=[]
    posFin=[]
    long=[]
    for i in intens.index:
        if ' '+intens['palabra'][i].upper()+' ' in post.upper():
            ids+=[event_id]
            pals_intens+=[intens['palabra'][i]]
            valor+=[intens['valor'][i]]
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
    if len(ids) > 0:
        dica={
                'ids':[ids],
                'pals_intens':[pals_intens],
                'valor_intens':[valor],
                'long_intens':[long],
                'posIni_intens':[posIni],
                'posFin_intens':[posFin]
              }
        DaFa=pd.DataFrame(dica)
        return(DaFa[['ids','pals_intens','valor_intens','posIni_intens','posFin_intens','long_intens']])
    else:
        return(False)
       
def buil_DaFr_intens(df_pals_mod):
    start=time.strftime("%H:%M:%S")
    DaFr_Intens=pd.DataFrame()
    intens=importa_intensificadores()
    for k in df_pals_mod.index:
        if recorre_intensificadores(intens,df_pals_mod['MSG'][k],df_pals_mod['id'][k]) is not False:
            DaFr_Intens=DaFr_Intens.append(recorre_intensificadores(intens,df_pals_mod['MSG'][k],df_pals_mod['id'][k]),ignore_index=True)
    x1=[]
    for k in DaFr_Intens.index:
        x1+=[DaFr_Intens['ids'][k][0]]
    DaFr_Intens['ids']=x1
    end=time.strftime("%H:%M:%S")
    print(' buil_DaFr_intens()','Inicio:  ',start,'Fin:  ',end)
    return(DaFr_Intens)

#if __name__ == '__main__':
#   DF_Ref,DF_Reproc,M1,M2,df_pals_mod=process.reproceso_delimitacion()
#   intens=importa_intensificadores()
#   DaFr_IntensT=buil_DaFr_intens(df_pals_mod)
#   print(DaFr_IntensT)

