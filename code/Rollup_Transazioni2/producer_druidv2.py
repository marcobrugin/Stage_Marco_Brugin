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

for i in range(20):
 a=locazione[random.randint(0,99)]
 citta=a[0]
 stato=a[1] 
 utenti=[[fake.first_name(), fake.last_name(),fake.date_of_birth(minimum_age=18, maximum_age=89).strftime("%Y-%m-%d"), citta, stato, fake.random_element(elements=("Scuola Secondaria", "Laurea triennale", "Laurea Magistrale", "Dottorato")), fake.random_element(elements=("Leggere","Viaggiare","Giocare a calcio","Giocare ai videogiochi","Fare sport")) ] for _ in range(150)]
volume=[]
for n in range(50):
  
   for j in range(100000):
        accesso=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        nome=utenti[random.randint(0,499)][0]
        cognome=utenti[random.randint(0,499)][1]
        datan=utenti[random.randint(0,499)][2]
        citta=utenti[random.randint(0,499)][3]
        stato=utenti[random.randint(0,499)][4]
        istruzione=utenti[random.randint(0,499)][5]
        hobby=utenti[random.randint(0,499)][6]
        my_data = {"accesso": accesso, "nome": nome, "cognome": cognome, "datan":  datan, "citta": citta, "stato": stato, "istruzione": istruzione,
        "hobby": hobby
        }
        producer.send("rollup1", value = my_data) 
        element=[nome, cognome, citta, stato, datan,istruzione, hobby,accesso]
        volume.append(element)
with open('data.csv', 'w', newline='') as file:
    writer = csv.writer(file, quoting=csv.QUOTE_NONNUMERIC, delimiter=',')
    writer.writerows(volume) 

print("End")
     
