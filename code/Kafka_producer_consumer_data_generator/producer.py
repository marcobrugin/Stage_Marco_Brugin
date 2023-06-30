from time import sleep  
from json import dumps  
from kafka import KafkaProducer  
import random
import datetime
import json

producer = KafkaProducer(  
    bootstrap_servers = ['localhost:9092', 'localhost:9093','localhost:9094'],  
    value_serializer = lambda x:json.dumps(x).encode('utf-8')  
    )  
print("Conncet succefully") 

a=["primo", "secondo", "terzo", "quarto", "quinto", "sesto", "settimo", "ottavo", "nono", "decimo"]
for n in range(10):  
	for j in range(50000):
	    my_data = {"timestamp": str(datetime.datetime.now()), "id" : str(random.randint(0,999)), "value": random.randint(0,1), "campo1": str(random.randrange(0,9999)), "campo2": a[random.randint(0, len(a)-1)], "campo3": random.randint(0,999), "campo4": random.randint(1000, 1999)} 
	    producer.send('druid_1', value = my_data) 
	print("Send")
	sleep(1) 
print("End")