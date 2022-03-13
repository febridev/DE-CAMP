import csv
from json import dumps
from tokenize import String
from kafka import KafkaProducer
from time import sleep


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         key_serializer=lambda x: dumps(x).encode('utf-8'),
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

file = open('data/marvel_reviews_clean.csv')

csvreader = csv.reader(file)
header = next(csvreader)
for row in csvreader:
    key = {"Film": str(row[0])}
    value = {"Film": str(row[0]), "Metacritic": float(row[2])}
    producer.send('homework.marvelrate.json', value=value, key=key)
    print("producing")
    sleep(50)