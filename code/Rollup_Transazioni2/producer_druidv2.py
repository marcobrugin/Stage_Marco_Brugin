from time import sleep  
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
max=random.randint(2,5)
nomi= [fake.first_name() for _ in range(max)]
max=random.randint(2,5)
cognomi= [fake.last_name() for _ in range(max)]
max=random.randint(2,5)
indirizzi= [fake.address() for _ in range(max)]
#la libreria faker genera indirizzi con \n, che vanno sostituiti con uno spazio
for i in range(len(indirizzi)):
    indirizzi[i]=indirizzi[i].replace('\n', ' ')
max=random.randint(2,7)
_citta=[fake.city() for _ in range(max)]
max=random.randint(2,7)
stati=[fake.country() for _ in range(max)]
max=random.randint(2,7)
_cap=[fake.zipcode() for _ in range(max)]
max=random.randint(2,7)
_email=[fake.email() for _ in range(max)]
max=random.randint(2,7)
telefoni=[fake.phone_number() for _ in range(max)]
max=random.randint(2,7)
_eta= [random.randint(18, 89) for _ in range(max)]
max=random.randint(2,7)
altezze= [round(random.uniform(120, 210), 2) for _ in range(max)]
max=random.randint(2,7)
pesi= [round(random.uniform(30, 180), 2) for _ in range(max)]
max=random.randint(2,7)
redditi= [round(random.uniform(1000, 10000), 2) for _ in range(max)]
max=random.randint(2,7)
daten= [fake.date_of_birth(minimum_age=18, maximum_age=89).strftime("%Y-%m-%d") for _ in range(max)]
max=random.randint(2,7)
professioni= [fake.job() for _ in range(max)]
max=random.randint(2,7)
_nfigli= [random.randint(0, 5) for _ in range(max)]
max=random.randint(2,7)
codici_cliente=[fake.random_number(digits=6) for _ in range(max)]
max=random.randint(2,7)
datereg= [fake.date_time_between(start_date="-1y", end_date="now").strftime("%Y-%m-%d %H:%M:%S") for _ in range(max)]
volume=[]
for n in range(500):
    for j in range(10000):
        nome= random.choice(nomi)
        cognome= random.choice(cognomi)
        indirizzo= random.choice(indirizzi)
        citta= random.choice(_citta)
        stato= random.choice(stati)
        cap= random.choice(_cap)
        email= random.choice(_email)
        telefono= random.choice(telefoni)
        eta= random.choice(_eta)
        altezza= random.choice(altezze)
        peso= random.choice(pesi)
        reddito= random.choice(redditi)
        datan= random.choice(daten)
        professione= random.choice(professioni)
        istruzione= fake.random_element(elements=("Scuola Secondaria", "Laurea triennale", "Laurea Magistrale", "Dottorato"))
        hobby= fake.random_element(elements=("Leggere","Viaggiare","Giocare a calcio","Giocare ai videogiochi","Fare sport"))
        nfigli= random.choice(_nfigli)
        codice_cliente= random.choice(codici_cliente)
        datareg= random.choice(datereg)
        accesso= datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        my_data = {"Accesso": accesso, "nome": nome, "cognome": cognome, "indirizzo": indirizzo, "citta": citta, "stato": stato, "cap": cap,
        "email": email, "telefono": telefono, "eta": eta, "altezza": altezza, "peso": peso,
        "reddito":reddito, "datan":  datan, "professione":  professione, "istruzione": istruzione,
        "hobby": hobby,
        "nfigli": nfigli, "nfigli":codice_cliente, 
        "datareg": datareg, 
        }
        producer.send("accessi", value = my_data) 
        element=[nome, cognome, indirizzo, citta, stato, cap, email, telefono, eta, altezza, peso, reddito, datan, professione, istruzione, hobby, nfigli, codice_cliente, datareg, accesso]
        volume.append(element)
        sleep(1)
with open('data.csv', 'w', newline='') as file:
    writer = csv.writer(file, quoting=csv.QUOTE_NONNUMERIC, delimiter=',')
    writer.writerows(volume) 

print("End")
     
