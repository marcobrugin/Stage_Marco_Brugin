from kafka import KafkaProducer  
import json 
import random
import csv
from faker import Faker
import datetime
producer = KafkaProducer(  
    bootstrap_servers = ["localhost:29092"],  
    value_serializer = lambda x:json.dumps(x).encode("utf-8")  
    )  

print("Connect succefully") 
fake = Faker()
locazione=[[fake.city(), fake.country()] for _ in range(200)]

utenti=list()
for i in range(150):
    a=locazione[random.randint(0,99)]
    citta=a[0]
    stato=a[1]
    a=random.randint(0,999)
    utenti.append([a, fake.first_name(), fake.last_name(),fake.date_of_birth(minimum_age=18, maximum_age=89).strftime("%Y-%m-%d"), citta, stato] )
volume=[]
for n in range(500):
    accesso=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    codice=utenti[random.randint(0,149)][0]
    nome=utenti[random.randint(0,149)][1]
    cognome=utenti[random.randint(0,149)][2]
    datan=utenti[random.randint(0,149)][3]
    citta=utenti[random.randint(0,149)][4]
    stato=utenti[random.randint(0,149)][5]
    my_data = {"accesso": accesso, "codice_cliente": codice,  "datan":  datan, "citta": citta, "stato": stato,
    }
    producer.send("lookup", value = my_data) 
    element=[codice, nome, cognome, citta, stato, datan, accesso]
    volume.append(element)
with open('data.csv', 'w', newline='') as file:
    writer = csv.writer(file, quoting=csv.QUOTE_NONNUMERIC, delimiter=',')
    writer.writerows(volume) 
print(utenti)
print("End")