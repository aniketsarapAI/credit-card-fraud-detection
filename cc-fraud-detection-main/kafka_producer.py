import json
import time
from csv import DictReader
from kafka import KafkaProducer

'''
This is a Kafka Producer, which reads data from store csv and push it on a topic named "spark03"
'''
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

'''
Reading Csv Data
'''
with open('creditcard.csv','r') as read_obj:
    csv_dict_reader = DictReader(read_obj)
    for row in csv_dict_reader:
        # sending data to the producer
        ack=producer.send('spark03', value=json.dumps(row).encode('utf-8'))
        metadata = ack.get()
        print(metadata.topic, metadata.partition)
