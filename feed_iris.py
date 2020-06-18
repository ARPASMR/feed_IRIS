# Alimentazione di IRIS
# logica di funzionamento: il programma legge le variabili di ambiente e può eseguire l'alimentazione in diverse modalità
# a. diretta: non ci sono ancora dati e vengono acquisiti direttamente dal REM (datainizio e datafine coincidono)
# b. in recupero: possono esserci delle lacune che vengono identificate e il dato corrispondente richiesto al REMWS_GATEWAY
#    in questo caso il recupero va da datainizio a datafine
# 1. inizializzazione
# 2. richiesta elenco sensori al db (df_sensori)

# FASE 1
#inizializzazione
import os,sys
import pandas as pd
# import numpy as np
from sqlalchemy import *
import datetime as dt
import json as js
import requests
# variabili di ambiente (da togliere in produzione)

#DEBUG=False
IRIS_TABLE_NAME='m_osservazioni_tr'
IRIS_SCHEMA_NAME='realtime'
AUTORE=os.getenv('COMPUTERNAME')
MINUTES=5 # minuti di recupero
if (AUTORE==None):
    AUTORE=os.getenv('NAME')
    IRIS_USER_ID=os.getenv('IRIS_USER_ID')
    IRIS_USER_PWD=os.getenv('IRIS_USER_PWD')
    IRIS_DB_NAME=os.getenv('IRIS_DB_NAME')
    IRIS_DB_HOST=os.getenv('IRIS_DB_HOST')
    h=os.getenv('TIPOLOGIE') # elenco delle tipologie da cercare nella tabella delle osservazioni realtime, è una stringa
    DEBUG=eval(os.getenv('DEBUG'))
    MINUTES=int(os.getenv('MINUTES')) #il valore viene sovrascritto dalla variabile d'ambiente (paramentro in launch_feed.sh)
    REMWS_GATEWAY=os.getenv('REMWS_GATEWAY')
    TIMEOUT=int(os.getenv('TIMEOUT'))
    # trasformo la stringa in lista
if (DEBUG):
    print("...inizio processo...")
url=REMWS_GATEWAY
TIPOLOGIE=h.split()
# inizializzazione delle date: datafine è in UTC+1
datafine=dt.datetime.utcnow()+dt.timedelta(minutes=60)
datainizio=datafine-dt.timedelta(minutes=MINUTES)
#definizione delle funzioni
# la funzione legge il blocco di dati e lo trasforma in DataFrame
def seleziona_richiesta(Risposta):
    # definisco dataframe della risposta
    df_risposta=pd.DataFrame(columns=['data_e_ora','misura','validita'])
    # dizionario di appoggio con la selezione dei dati
    aa=Risposta['data']['sensor_data_list'][0]['data']
    # ciclo lettura
    for i in range(1,len(aa)-1):
    #    print(i,aa[i]['datarow'].split(";")[2])
        df_risposta.loc[i-1]=[aa[i]['datarow'].split(";")[0],aa[i]['datarow'].split(";")[1],aa[i]['datarow'].split(";")[2]]
    return df_risposta
def Inserisci_in_realtime(schema,table,idsensore,tipo,operatore,datar,misura,autore):
    # la funzione crea la query da per l'inserimento del dato
    s=dt.datetime.now()
    mystring=s.strftime("%Y-%m-%d %H:%M")
    Query_Insert="INSERT into "+schema+"."+table+\
    " (idsensore,nometipologia,idoperatore,data_e_ora,misura, autore,data)\
    VALUES ("+str(idsensore)+",'"+tipo+"',"+str(operatore)+",'"+\
    datar.strftime("%Y-%m-%d %H:%M")+"',"+str(misura)+",'"+ autore+"','"+mystring+"');"
    return Query_Insert
def Richiesta_remwsgwy (framedati):
    #funzione di colloquio con il remws: manda la dichiesta e decodifica la risposta
    global TIMEOUT,esito
    richiesta={
        'header':{'id': 13},
        'data':{'sensors_list':[framedati]}
        }
    ci_sono_dati=False
    try:
       r=requests.post(url,data=js.dumps(richiesta),timeout=TIMEOUT)
       esito['richiesti']+=1
       if(len(r.text)>0):
          risposta=js.loads(r.text)
          #controllo progressivamente se la risposta è buona e se ci sono dati
          outcome=risposta['data']['outcome']
          if (outcome==0):
            if (len(risposta['data']['sensor_data_list'])>0):
                candidate=risposta['data']['sensor_data_list'][0]['data']
                for j in candidate:
                    k=j['datarow'].split(";")
                    if (len(k)==3):
                        ora=k[0]
                        misura=k[1]
                        valido=k[2]
                        if(int(valido)>=0):
                            ci_sono_dati=True
                 # chiude ciclo esame dati
       else:
            esito['mancanti']+=1
            return []
    except:
        esito['errori']+=1
        if(DEBUG):
            print("Errore: REMWS non raggiungibile", end="\r")

    if(ci_sono_dati):
        # estraggo il dato
        esito['ricevuti']+=1
        return candidate
    else:
        esito['mancanti']+=1
        return []
###

#FASE 2 - query al dB
engine = create_engine('postgresql+pg8000://'+IRIS_USER_ID+':'+IRIS_USER_PWD+'@'+IRIS_DB_HOST+'/'+IRIS_DB_NAME)
conn=engine.connect()

#preparazione dell'elenco dei sensori
Query='Select *  from "dati_di_base"."anagraficasensori" where "anagraficasensori"."datafine" is NULL and idrete in (1,2,4);'
df_sensori=pd.read_sql(Query, conn)
if (DEBUG):
    print("...accesso al dB sensori eseguito....")

#ALIMETAZIONE DIRETTA
# suppongo di non avere ancora chisto dati, vedo qule dato devo chiedere, lo chiedo e loinserisco.
# Se l'inserimento fallisce vuol dire che qualcun altro ha inserito il dato (ovvero un processo in parallelo, il che èstrano...)

# selezione dell'ora
minuto=int(datainizio.minute/10)*10
data_ricerca=dt.datetime(datainizio.year,datainizio.month,datainizio.day,datainizio.hour,minuto,0)
ora=dt.datetime(datainizio.year,datainizio.month,datainizio.day,datainizio.hour,0,0)
df_section=df_sensori[df_sensori.nometipologia.isin(TIPOLOGIE)].sample(frac=1)
# aggiunto sort casuale per parallelizzazione
if (DEBUG):
    print("...inizio ciclo sensori...")
#ciclo sui sensori:
# strutturo la richiesta
id_operatore=1
function=1
frame_dati={}
frame_dati["sensor_id"]=0
frame_dati["granularity"]=1 # chiedo solo i 10 minuti
frame_dati["start"]=data_ricerca.strftime("%Y-%m-%d %H:%M")
frame_dati["finish"]=data_ricerca.strftime("%Y-%m-%d %H:%M")
#suppongo che in df_section ci siano solo i sensori che mi interessano e faccio il ciclo di richiesta
s=dt.datetime.now()
conn=engine.connect()
regole={}
esito={'richiesti':0,'ricevuti':0,'inseriti':0,'errori':0,'mancanti':0,'db_err':0}
# inizio del ciclo vero e proprio
for row in df_section.itertuples():
    # controllo quanto tempo è passato: le alimentazioni possono durare al massimo 10'
    timeDiff=dt.datetime.now()-s
    durata_script=timeDiff.total_seconds() / 60
    if (durata_script>10):
        print(f"Esito {esito} per {TIPOLOGIE} inizio {s} fine {dt.datetime.now()}")
        sys.exit("Esecuzione troppo lunga - interrompo!")
    frame_dati["sensor_id"]=row.idsensore
    data_insert=data_ricerca
    # assegno operatore e funzione corretti
    # riepilogo casi:
    # frequenza 1 minuti (pluviometro CAE): function=3, idperiodo=1
    # frequenza 5 minuti (pluviometri ETG): function=3, idperiodo=1
    # frequenza 10 minuti (pluviometri PA): function=1, idperiodo=1
    #
     # selezione degli idrometri con frequenza 5 minuti
    if(row.frequenza==60):
        id_periodo=3
        frame_dati["start"]=ora.strftime("%Y-%m-%d %H:%M")
        frame_dati["finish"]=ora.strftime("%Y-%m-%d %H:%M")
        data_insert=ora
    else:
    # selezione degli idrometri con frequenza 5 minuti
        if (row.frequenza==5):
             id_operatore=1
             id_periodo=10
        else:
             function=1
             id_periodo=1
        frame_dati["start"]=data_ricerca.strftime("%Y-%m-%d %H:%M")
        frame_dati["finish"]=data_ricerca.strftime("%Y-%m-%d %H:%M")
    if(row.nometipologia=='PP'):
        id_operatore=4
        function=3
        if(row.frequenza>5):
            function=1
    else:
         id_operatore=1
         function=1

    frame_dati["operator_id"]=id_operatore
    frame_dati["function_id"]=function
    frame_dati["granularity"]=id_periodo
    aa=Richiesta_remwsgwy(frame_dati)
    if (len(aa)>2):
        # prendo solo il primo elemento
        misura=aa[1]['datarow'].split(";")[1]
        h=valido=aa[1]['datarow'].split(";")[2]

        QueryInsert=Inserisci_in_realtime(IRIS_SCHEMA_NAME,IRIS_TABLE_NAME,\
        row.idsensore,row.nometipologia,id_operatore,data_insert,misura,AUTORE)
        try:
            conn.execute(QueryInsert)
            if (DEBUG):
                print("+++",row.idsensore,data_insert,misura)
        except:
            if(DEBUG):
                print("Query non riuscita! per ",row.idsensore)
    else:
        if (DEBUG):
            print ("Attenzione: dato di ",TIPOLOGIE, "sensore ", row.idsensore,data_insert, "ASSENTE nel REM")
    # prima di chiudere il ciclo chiedo la raffica del vento
    if(row.nometipologia=='VV' or row.nometipologia=='DV'):
        id_operatore=3
        frame_dati["operator_id"]=id_operatore
        aa=Richiesta_remwsgwy(frame_dati)
        if (len(aa)>2):
        # prendo solo il primo elemento
            misura=aa[1]['datarow'].split(";")[1]
            valido=aa[1]['datarow'].split(";")[2]
            QueryInsert=Inserisci_in_realtime(IRIS_SCHEMA_NAME,IRIS_TABLE_NAME,\
            row.idsensore,row.nometipologia,id_operatore,data_insert,misura,AUTORE)
            try:
                conn.execute(QueryInsert)
                esito['inseriti']+=1
                if (DEBUG):
                    print("+++",row.idsensore,data_ricerca,misura)
            except:
                esito['db_err']+=1
                if(DEBUG):
                    print("Query non riuscita! per ",row.idsensore)
        else:
            if (DEBUG):
                print ("Attenzione: dato di ",TIPOLOGIE, "sensore ", row.idsensore, "ASSENTE nel REM")
    #fine ciclo sensore
print(f"Esito {esito} per {TIPOLOGIE} inizio {s} fine {dt.datetime.now()}")
