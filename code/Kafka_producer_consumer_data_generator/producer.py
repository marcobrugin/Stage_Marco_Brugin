from time import sleep  
from json import dumps  
from kafka import KafkaProducer  
import random

producer = KafkaProducer(  
    bootstrap_servers = ['localhost:9092', 'localhost:9093','localhost:9094'],  
    value_serializer = lambda x:dumps(x).encode('utf-8')  
    )  
print("Conncet succefully") 

for n in range(50):  
    for j in range(10):
	    my_data = {'id' : str(random.randint(0,999)),'value': random.randint(0,1)}  
	    producer.send('test', value = my_data)  
    sleep(1) 
print("End")
     
