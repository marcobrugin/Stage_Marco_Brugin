from time import sleep  
from kafka import KafkaProducer  
import json 
import random
import datetime
import csv
from faker import Faker

producer = KafkaProducer(  
    bootstrap_servers = ["localhost:29092"],  
    value_serializer = lambda x:json.dumps(x).encode("utf-8")  
    )  
print("Conncet succefully") 
fake = Faker()
volume=[]
for n in range(500000): 
    nome= fake.first_name()
    cognome= fake.last_name()
    indirizzo= fake.address()
    citta= fake.city()
    stato= fake.country()
    cap= fake.zipcode()
    email= fake.email()
    telefono= fake.phone_number()
    eta= random.randint(18, 89)
    altezza= round(random.uniform(120, 210), 2)
    peso= round(random.uniform(30, 180), 2)
    reddito= round(random.uniform(1000, 10000), 2)
    datan= fake.date_of_birth(minimum_age=18, maximum_age=89).strftime("%Y-%m-%d")
    professione= fake.job()
    istruzione= fake.random_element(elements=("Scuola Secondaria", "Laurea triennale", "Laurea Magistrale", "Dottorato"))[0]
    hobby= fake.random_element(elements=("Leggere","Viaggiare","Giocare a calcio","Giocare ai videogiochi","Fare sport"))
    nfigli= random.randint(0, 5)
    codice_cliente= fake.random_number(digits=6)
    datareg= fake.date_time_between(start_date="-1y", end_date="now").strftime("%Y-%m-%d %H:%M:%S")
    ultimoacc= fake.date_time_between(start_date="-1w", end_date="now").strftime("%Y-%m-%d %H:%M:%S")
    my_data = {"Nome": nome, "Cognome": cognome, "Indirizzo": indirizzo, "Città": citta, "Stato": stato, "CAP": cap,
    "Email": email, "Telefono": telefono, "Età": eta, "Altezza": altezza, "Peso": peso,
    "Reddito":reddito, "Data di Nascita":  datan, "Professione":  professione, "Istruzione": istruzione,
    "Hobby": hobby,
    "Numero di Figli": nfigli, "Codice Cliente":codice_cliente, 
    "Data di Registrazione": datareg, 
    "Ultimo Accesso": ultimoacc}
    producer.send("prova1", value = my_data) 

    element=[nome, cognome, indirizzo, citta, stato, cap, email, telefono, eta, altezza, peso, reddito, datan, professione, istruzione, hobby, nfigli, codice_cliente, datareg, ultimoacc]
    volume.append(element)
    print("Send")
    sleep(1)
with open("data2.csv", "w", newline="") as file:
    writer = csv.writer(file, quoting=csv.QUOTE_NONNUMERIC, delimiter=",")
    writer.writerows(volume) 
print("End")
     
