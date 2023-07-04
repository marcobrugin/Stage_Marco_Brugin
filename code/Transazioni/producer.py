from time import sleep  
from json import dumps  
from kafka import KafkaProducer  
import random
import datetime
import json

producer = KafkaProducer(  
    bootstrap_servers = ['localhost:29092'],  
    value_serializer = lambda x:json.dumps(x).encode('utf-8')  
    )  
print("Conncet succefully") 
cliente=['Mario','Luigi','Pippo','Pluto','Paperino','Paperone','Qui','Quo','Qua','Zio Paperone']
prodotto=["primo", "secondo", "terzo", "quarto", "quinto", "sesto", "settimo", "ottavo", "nono", "decimo"]
for n in range(10):  
	for j in range(1000):
	    my_data = {"timestamp": str(datetime.datetime.now()), "cliente" : cliente[random.randint(0,len(cliente)-1)], "prodotto": prodotto[random.randint(0,len(prodotto)-1)], "quantita": random.randint(0,999), "gradimento": random.randint(0, 10)} 
	    producer.send('druid', value = my_data) 
	print("Send")
	sleep(1) 
print("End")
     
