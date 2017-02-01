# -*- coding: utf-8 -*-
"""
Created on Thu Jul 14 20:51:04 2016
@author: AAREYESC
"""

import codecs;

#fichero para leer
try:
    r=codecs.open("GaleriasRS_Referencia.csv","r","utf8")
except Exception as msg:
    print(msg)
    r=codecs.open("GaleriasRS_Referencia.csv","r")
#ficheros a escribir
try:
    f=codecs.open("s.csv","w","utf8") #escribimos en el otro fichero
except Exception as msg:
    print(msg)
    f=codecs.open("s.csv","w") 
try:
    g=codecs.open("GaleriasRS_Referencia_limpio.csv","w","utf8") #escribimos en el otro fichero
except Exception as msg:
    print(msg)
    g=codecs.open("GaleriasRS_Referencia_limpio.csv","w")

cnt=0
regs=0
for i in r:
    f.write(i.replace('\n','').replace('\r',''))
    regs+=1
    if (regs % 1000) == 0:
        print ('cuenta de registros: ',regs)

print ('cuenta de registros totales: ',regs)       
f.close()
r.close()

arc_med=codecs.open("s.csv","r","utf8")
cadena = arc_med.readline()
cadena_num=cadena.count("\t")

count2=0
cad=''
count3=0
for i in cadena:
    cad=cad+i
    if i =='\t':
        count2+=1
        if (count2 % 29) == 0:
            #g.write(cad+'\n')
            spt=cad.split('\t')
            event_id_no=spt[0]
            Fecha_de_Creacion=spt[1]
            Usuarios_que_Interactuan=spt[2]
            Redes_Sociales_Id=spt[3]
            Nivel_del_Miembro=spt[4]
            Centros_Comerciales=spt[5]
            Redes_Sociales=spt[6]
            Entidades_Companias=spt[7]
            Entidades_Negativas=spt[8]
            Entidades_Neutrales=spt[9]
            Entidades_Persona=spt[10]
            Entidades_Positivas=spt[11]
            Entidades_Servicios=spt[12]
            Fecha_de_Actualizacion_del_Mensaje_en_Facebook=spt[13]
            Id_de_Posteo=spt[14]
            Mensaje=spt[15]
            Hashtag=spt[16]
            Temas=spt[17]
            Sentimiento=spt[18]
            Campanas_Eventos=spt[19]
            Interaccion_Tipo=spt[20]
            Jerarquia_Comentario_FB=spt[21]
            Me_Gusta_Favorito=spt[22]
            Compartir_Retweet=spt[23]
            Permalink=spt[24]
            Link_Externo=spt[25]
            Atencion_Al_Cliente=spt[26]
            Siebel_Id=spt[27]
            Lealtad=spt[28]
            #print event_id_no,fecha_creacion,usuarios_Interactuan,facebook_id_crm,redes_sociales_id,nivel_miembro,centros_comerciales,redes_sociales,
            #visitas,fecha_actualizacion_mensaje_facebook,id_posteo,mensaje,hashtag,temas,sentimiento,campanas_ventos,interaccion_tipo,
            #jerarquia_comentario_fb,me_gusta_favorito,compartir_retweet,permalink,link_externo,atencion_cliente,siebel_id,lealtad
            vect="""%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t"""% (event_id_no,Fecha_de_Creacion,Usuarios_que_Interactuan,Redes_Sociales_Id,Nivel_del_Miembro,Centros_Comerciales,Redes_Sociales,Entidades_Companias,
               Entidades_Negativas,Entidades_Neutrales,Entidades_Persona,Entidades_Positivas,Entidades_Servicios,Fecha_de_Actualizacion_del_Mensaje_en_Facebook,
               Id_de_Posteo,Mensaje,Hashtag,Temas,Sentimiento,Campanas_Eventos,Interaccion_Tipo,Jerarquia_Comentario_FB,Me_Gusta_Favorito,Compartir_Retweet,
               Permalink,Link_Externo,Atencion_Al_Cliente,Siebel_Id,Lealtad)
            if count2>29:
                g.write(vect+'\n')
            #c.execute(sql)
            
            cad=''

count2
g.close()           
print('El archivo esta listo para abrirse: ')




