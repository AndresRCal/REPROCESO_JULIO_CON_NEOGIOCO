# -*- coding: utf-8 -*-
"""
Created on Thu Feb  2 12:48:56 2017

@author: AAREYESC
"""

def replacing_substrings(post,lst_comienzan=['htt','#','@','RT']):
    substrings=[]
    for i in lst_comienzan:
        pos_ini=[]
        pos_fin=[]
        band=0
#        print(i)
        cnt=post.count(i)
#        print(cnt)
        if cnt > 0:
            while band < cnt:
                if band == 0:
                    pos_ini+=[post.index(i)]
                else:
                    pa_salir=pos_ini[band-1]+1
                    pos_ini+=[post.index(i,pa_salir)]
#                print(pos_ini)
                try:
                    pos_fin+=[post.index(' ',pos_ini[band])]
                except Exception as msg:
#                    print(msg)
                    pos_fin+=[len(post)]                              
                substrings+=[post[pos_ini[band]:pos_fin[band]]]
                band=band+1              
        else: 
            pass
    post2=post
    if len(substrings) > 0:
        for j in substrings:
            post2=post2.replace(j,'')
        return(substrings,post2)
    else:
        return(False,post2)
        
def finding_substrings(post,lst_comienzan=['htt','#','@','RT']):
    substrings=[]
    for i in lst_comienzan:
        pos_ini=[]
        pos_fin=[]
        band=0
#        print(i)
        cnt=post.count(i)
#        print(cnt)
        if cnt > 0:
            while band < cnt:
                if band == 0:
                    pos_ini+=[post.index(i)]
                else:
                    pa_salir=pos_ini[band-1]+1
                    pos_ini+=[post.index(i,pa_salir)]
#                print(pos_ini)
                try:
                    pos_fin+=[post.index(' ',pos_ini[band])]
                except Exception as msg:
#                    print(msg)
                    pos_fin+=[len(post)-1]                              
                substrings+=[post[pos_ini[band]:pos_fin[band]]]
                band=band+1              
        else: 
            pass
    if len(substrings) > 0:
        return(substrings)
    else:
        return(False)
    
