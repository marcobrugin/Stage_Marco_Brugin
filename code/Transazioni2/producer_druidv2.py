from time import sleep  
#from kafka import KafkaProducer  
import json 
import random
import csv
from faker import Faker
'''
producer = KafkaProducer(  
    bootstrap_servers = ["localhost:29092"],  
    value_serializer = lambda x:json.dumps(x).encode("utf-8")  
    )  
    '''
print("Conncet succefully") 
fake = Faker()
max=random.randint(4,20)
nomi= [fake.first_name() for _ in range(max)]
max=random.randint(4,20)
cognomi= [fake.last_name() for _ in range(max)]
max=random.randint(4,20)
indirizzi= [fake.address() for _ in range(max)]
#la libreria faker genera indirizzi con \n, che vanno sostituiti con uno spazio
for i in range(len(indirizzi)):
    indirizzi[i]=indirizzi[i].replace('\n', ' ')
max=random.randint(4,20)
cittas=[fake.city() for _ in range(max)]
max=random.randint(4,20)
stati=[fake.country() for _ in range(max)]
max=random.randint(4,20)
caps=[fake.zipcode() for _ in range(max)]
max=random.randint(4,20)
emails=[fake.email() for _ in range(max)]
max=random.randint(4,20)
telefoni=[fake.phone_number() for _ in range(max)]
max=random.randint(4,20)
etas= [random.randint(18, 89) for _ in range(max)]
max=random.randint(4,20)
altezzas= [round(random.uniform(120, 210), 2) for _ in range(max)]
max=random.randint(4,20)
pesos= [round(random.uniform(30, 180), 2) for _ in range(max)]
max=random.randint(4,20)
redditi= [round(random.uniform(1000, 10000), 2) for _ in range(max)]
max=random.randint(4,20)
datans= [fake.date_of_birth(minimum_age=18, maximum_age=89).strftime("%Y-%m-%d") for _ in range(max)]
max=random.randint(4,20)
professioni= [fake.job() for _ in range(max)]
max=random.randint(4,20)
nfiglis= [random.randint(0, 5) for _ in range(max)]
max=random.randint(4,20)
codice_clienti=[fake.random_number(digits=6) for _ in range(max)]
max=random.randint(4,20)
dataregs= [fake.date_time_between(start_date="-1y", end_date="now").strftime("%Y-%m-%d %H:%M:%S") for _ in range(max)]
max=random.randint(4,20)
ultimoaccs=[fake.date_time_between(start_date="-1w", end_date="now").strftime("%Y-%m-%d %H:%M:%S") for _ in range(max)]
max=random.randint(4,20)
volume=[]
for n in range(500):
    for j in range(10000):
        nome= random.choice(nomi)
        cognome= random.choice(cognomi)
        indirizzo= random.choice(indirizzi)
        citta= random.choice(cittas)
        stato= random.choice(stati)
        cap= random.choice(caps)
        email= random.choice(emails)
        telefono= random.choice(telefoni)
        eta= random.choice(etas)
        altezza= random.choice(altezzas)
        peso= random.choice(pesos)
        reddito= random.choice(redditi)
        datan= random.choice(datans)
        professione= random.choice(professioni)
        istruzione= fake.random_element(elements=("Scuola Secondaria", "Laurea triennale", "Laurea Magistrale", "Dottorato"))
        hobby= fake.random_element(elements=("Leggere","Viaggiare","Giocare a calcio","Giocare ai videogiochi","Fare sport"))
        nfigli= random.choice(nfiglis)
        codice_cliente= random.choice(codice_clienti)
        datareg= random.choice(dataregs)
        ultimoacc= random.choice(ultimoaccs)
        my_data = {"Nome": nome, "Cognome": cognome, "Indirizzo": indirizzo, "Città": citta, "Stato": stato, "CAP": cap,
        "Email": email, "Telefono": telefono, "Età": eta, "Altezza": altezza, "Peso": peso,
        "Reddito":reddito, "Data di Nascita":  datan, "Professione":  professione, "Istruzione": istruzione,
        "Hobby": hobby,
        "Numero di Figli": nfigli, "Codice Cliente":codice_cliente, 
        "Data di Registrazione": datareg, 
        "Ultimo Accesso": ultimoacc}
        #producer.send("registrazione1", value = my_data) 

        element=[nome, cognome, indirizzo, citta, stato, cap, email, telefono, eta, altezza, peso, reddito, datan, professione, istruzione, hobby, nfigli, codice_cliente, datareg, ultimoacc]
        volume.append(element)
        print("Send")
        #sleep(1)
with open("data2.csv", "w", newline="") as file:
    writer = csv.writer(file, quoting=csv.QUOTE_NONNUMERIC, delimiter=",")
    writer.writerows(volume) 
print("End")
     
