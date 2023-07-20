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
nomi= [fake.first_name() for _ in range(max_n)]
max_c=random.randint(2,4)
cognomi= [fake.last_name() for _ in range(max_c)]
max_i=random.randint(2,4)
indirizzi= [fake.address() for _ in range(max_i)]
#la libreria faker genera indirizzi con \n, che vanno sostituiti con uno spazio
for i in range(len(indirizzi)):
    indirizzi[i]=indirizzi[i].replace('\n', ' ')
max_ci=random.randint(2,4)
_citta=[fake.city() for _ in range(max_ci)]
max_s=random.randint(2,4)
stati=[fake.country() for _ in range(max_s)]
max_ca=random.randint(2,4)
_cap=[fake.zipcode() for _ in range(max_ca)]
max_e=random.randint(2,4)
_email=[fake.email() for _ in range(max_e)]
max_t=random.randint(2,4)
telefoni=[fake.phone_number() for _ in range(max_t)]
max_et=random.randint(2,4)
_eta= [random.randint(18, 89) for _ in range(max_et)]
max_a=random.randint(2,4)
altezze= [round(random.uniform(120, 210), 2) for _ in range(max_a)]
max_p=random.randint(2,4)
pesi= [round(random.uniform(30, 180), 2) for _ in range(max_p)]
max_r=random.randint(2,4)
redditi= [round(random.uniform(1000, 10000), 2) for _ in range(max_r)]
max_d=random.randint(2,4)
daten= [fake.date_of_birth(minimum_age=18, maximum_age=89).strftime("%Y-%m-%d") for _ in range(max_d)]
max_pr=random.randint(2,4)
professioni= [fake.job() for _ in range(max_pr)]
max_ni=random.randint(2,4)
_nfigli= [random.randint(0, 5) for _ in range(max_ni)]
max_co=random.randint(2,4)
codici_cliente=[fake.random_number(digits=6) for _ in range(max_co)]
max_dg=random.randint(2,4)
datereg= [fake.date_time_between(start_date="-1y", end_date="now").strftime("%Y-%m-%d %H:%M:%S") for _ in range(max_dg)]
volume=[]
for n in range(500):
    for j in range(10000):
        nome= random.choice([fake.first_name() for _ in range(max_n)])
        cognome= random.choice([fake.last_name() for _ in range(max_c)])
        indirizzo= random.choice([fake.address() for _ in range(max_i)])
        citta= random.choice([fake.city() for _ in range(max_ci)])
        stato= random.choice([fake.country() for _ in range(max_s)])
        cap= random.choice([fake.zipcode() for _ in range(max_ca)])
        email= random.choice([fake.email() for _ in range(max_e)])
        telefono= random.choice([fake.phone_number() for _ in range(max_t)])
        eta= random.choice([random.randint(18, 89) for _ in range(max_et)])
        altezza= random.choice([round(random.uniform(120, 210), 2) for _ in range(max_a)])
        peso= random.choice([round(random.uniform(30, 180), 2) for _ in range(max_p)])
        reddito= random.choice([round(random.uniform(1000, 10000), 2) for _ in range(max_r)])
        datan= random.choice([fake.date_of_birth(minimum_age=18, maximum_age=89).strftime("%Y-%m-%d") for _ in range(max_d)])
        professione= random.choice([fake.job() for _ in range(max_pr)])
        istruzione= fake.random_element(elements=("Scuola Secondaria", "Laurea triennale", "Laurea Magistrale", "Dottorato"))
        hobby= fake.random_element(elements=("Leggere","Viaggiare","Giocare a calcio","Giocare ai videogiochi","Fare sport"))
        nfigli= random.choice([random.randint(0, 5) for _ in range(max_ni)])
        codice_cliente= random.choice([fake.random_number(digits=6) for _ in range(max_co)])
        datareg= random.choice([fake.date_time_between(start_date="-1y", end_date="now").strftime("%Y-%m-%d %H:%M:%S") for _ in range(max_dg)])
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
     
