import csv
from json import dumps
from tokenize import String
from kafka import KafkaProducer
from time import sleep


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         key_serializer=lambda x: dumps(x).encode('utf-8'),
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

file = open('data/marvel_clean.csv')

csvreader = csv.reader(file)
header = next(csvreader)
for row in csvreader:
    key = {"Title": str(row[0])}
    value = {"title": str(row[0]), "distributor": str(row[1]), "budget": float(row[3])}
    producer.send('homework.marvelmovie.json', value=value, key=key)
    print("producing")
    sleep(50)