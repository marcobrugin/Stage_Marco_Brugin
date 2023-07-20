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
    for j in range(10000):
        nome= random.choice([fake.first_name() for _ in range(max_n)])
        cognome= random.choice([fake.last_name() for _ in range(max_c)])
        indirizzi= [fake.address() for _ in range(max_i)]
        for i in range(len(indirizzi)):
            indirizzi[i]=indirizzi[i].replace('\n', ' ')
        indirizzo= random.choice(indirizzi)
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
with open('data.csv', 'w', newline='') as file:
    writer = csv.writer(file, quoting=csv.QUOTE_NONNUMERIC, delimiter=',')
    writer.writerows(volume) 

print("End")
     
