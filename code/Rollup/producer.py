from time import sleep  
from json import dumps  
from kafka import KafkaProducer  
import random
import datetime
import json
import csv
producer = KafkaProducer(  
    bootstrap_servers = ['localhost:29092'],  
    value_serializer = lambda x:json.dumps(x).encode('utf-8')  
    )  
print("Conncet succefully") 
cliente=['Mario','Luigi','Pippo','Pluto','Paperino','Paperone','Qui','Quo','Qua','Zio Paperone']
prodotto=["primo", "secondo", "terzo", "quarto", "quinto", "sesto", "settimo", "ottavo", "nono", "decimo"]
volume=[]
for n in range(100):  
	for j in range(50):
	    data=str(datetime.datetime.now())
	    clienti=cliente[random.randint(0,len(cliente)-1)]
	    prodotti= prodotto[random.randint(0,len(prodotto)-1)]
	    quantita= random.randint(0,999)
	    gradimento=random.randint(0, 10)
	    my_data = {"timestamp": data, "cliente" : clienti, "prodotto": prodotti, "quantita": quantita, "gradimento": gradimento} 
	    producer.send('rollup', value = my_data) 
	    element=[data,clienti,prodotti,quantita,gradimento]
	    volume.append(element)
	print("Send")
	sleep(2)
with open('data.csv', 'w', newline='') as file:
    writer = csv.writer(file, quoting=csv.QUOTE_NONNUMERIC, delimiter=',')
    writer.writerows(volume) 
print("End")
     
