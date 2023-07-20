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
max_n=random.randint(2,4)

max_c=random.randint(2,4)

max_i=random.randint(2,4)

#la libreria faker genera indirizzi con \n, che vanno sostituiti con uno spazio

max_ci=random.randint(2,4)

max_s=random.randint(2,4)

max_ca=random.randint(2,4)

max_e=random.randint(2,4)

max_t=random.randint(2,4)

max_et=random.randint(2,4)

max_a=random.randint(2,4)

max_p=random.randint(2,4)

max_r=random.randint(2,4)

max_d=random.randint(2,4)

max_pr=random.randint(2,4)

max_ni=random.randint(2,4)

max_co=random.randint(2,4)

max_dg=random.randint(2,4)

volume=[]
for n in range(500):
    nomi= [fake.first_name() for _ in range(max_n)]
    cognomi= [fake.last_name() for _ in range(max_c)]
    indirizzi= [fake.address() for _ in range(max_i)]
    for i in range(len(indirizzi)):
        indirizzi[i]=indirizzi[i].replace('\n', ' ')
    _citta=[fake.city() for _ in range(max_ci)]
    stati=[fake.country() for _ in range(max_s)]
    _cap=[fake.zipcode() for _ in range(max_ca)]
    _email=[fake.email() for _ in range(max_e)]
    telefoni=[fake.phone_number() for _ in range(max_t)]
    _eta= [random.randint(18, 89) for _ in range(max_et)]
    altezze= [round(random.uniform(120, 210), 2) for _ in range(max_a)]
    pesi= [round(random.uniform(30, 180), 2) for _ in range(max_p)]
    redditi= [round(random.uniform(1000, 10000), 2) for _ in range(max_r)]
    daten= [fake.date_of_birth(minimum_age=18, maximum_age=89).strftime("%Y-%m-%d") for _ in range(max_d)]
    professioni= [fake.job() for _ in range(max_pr)]
    _nfigli= [random.randint(0, 5) for _ in range(max_ni)]
    codici_cliente=[fake.random_number(digits=6) for _ in range(max_co)]
    datereg= [fake.date_time_between(start_date="-1y", end_date="now").strftime("%Y-%m-%d %H:%M:%S") for _ in range(max_dg)]
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
with open('data.csv', 'w', newline='') as file:
    writer = csv.writer(file, quoting=csv.QUOTE_NONNUMERIC, delimiter=',')
    writer.writerows(volume) 

print("End")
     
