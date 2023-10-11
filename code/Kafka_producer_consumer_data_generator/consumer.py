from json import loads  
from kafka import KafkaConsumer  
import json

consumer = KafkaConsumer(  
    'druid',  
     bootstrap_servers = ['localhost:9092', 'localhost:9093','localhost:9094'],  
     auto_offset_reset = 'earliest',  
     enable_auto_commit = True,  
     group_id = 'my-group',  
     value_deserializer = lambda x : json.loads(x.decode('utf-8'))  
     )  
for message in consumer:  
    message = message.value  
    print(message)      
