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
max_n=random.randint(4,4)
max_c=random.randint(4,10)
max_i=random.randint(4,10)
#la libreria faker genera indirizzi con \n, che vanno sostituiti con uno spazio
max_ci=random.randint(4,10)
max_s=random.randint(4,10)
max_ca=random.randint(4,10)
max_e=random.randint(4,10)
max_t=random.randint(4,10)
max_et=random.randint(4,10)
max_a=random.randint(4,10)
max_p=random.randint(4,10)
max_r=random.randint(4,10)
max_d=random.randint(4,10)
max_pr=random.randint(4,10)
max_ni=random.randint(4,10)
max_co=random.randint(4,10)
max_dg=random.randint(4,10)
volume=[]
for n in range(50):
   nomi= [fake.first_name() for _ in range(max_n)]
   cognomi= [fake.last_name() for _ in range(max_c)]
   _citta=[fake.city() for _ in range(max_ci)]
   stati=[fake.country() for _ in range(max_s)]
   redditi= [round(random.uniform(1000, 10000), 2) for _ in range(max_r)]
   daten= [fake.date_of_birth(minimum_age=18, maximum_age=89).strftime("%Y-%m-%d") for _ in range(max_d)]
   for j in range(100000):
        nome= random.choice(nomi)
        cognome= random.choice(cognomi)
        citta= random.choice(_citta)
        stato= random.choice(stati)
        reddito= random.choice(redditi)
        datan= random.choice(daten)
        istruzione= fake.random_element(elements=("Scuola Secondaria", "Laurea triennale", "Laurea Magistrale", "Dottorato"))
        hobby= fake.random_element(elements=("Leggere","Viaggiare","Giocare a calcio","Giocare ai videogiochi","Fare sport"))
        accesso= datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        my_data = {"Accesso": accesso, "nome": nome, "cognome": cognome,"citta": citta, "stato": stato,
        "reddito":reddito, "datan":  datan, "istruzione": istruzione,
        "hobby": hobby
        }
        producer.send("druid_rollup", value = my_data) 
        element=[nome, cognome, citta, stato,reddito, datan,istruzione, hobby,accesso]
        volume.append(element)
with open('data.csv', 'w', newline='') as file:
    writer = csv.writer(file, quoting=csv.QUOTE_NONNUMERIC, delimiter=',')
    writer.writerows(volume) 

print("End")
     
