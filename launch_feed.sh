#!/bin/bash
# script di lancio dell'alimentazione di iris tramite pyhton
#
# il primo argomento è il minuto in cui eseguire il comando ogni 10 minuti
# il secondo argomento è in caso di recupero: mettere comunque una lettera diversa da 'R'
# in caso di recupero esegue lo script e POI si ferma per un tempo determinato dal terzo parametro
nomescript=${0##*/}
if [ $3 > 0 ]
then
   dormi=$3
else
  dormi=3600
fi
while [ 1 ]
do
   data_corrente=$[ 10#$(date +"%M") % 10 ]
   if [ $data_corrente == $1 ] && [ $2 != "R" ]
   then
    # logger -is -p user.notice "$nomescript: eseguo alimentazione al minuto $1 per $TIPOLOGIE"
     echo "Eseguo alimentazione diretta per $TIPOLOGIE"
     python3 feed_iris.py
   else
     sleep 30
   fi
   if [ $2 == "R" ]
   then
     echo "Eseguo recupero per $TIPOLOGIE"
    # logger -is -p user.notice "$nomescript: eseguo recupero per $TIPOLOGIE"
     python3 feed_iris_recupero.py
     sleep $dormi
   fi
done
