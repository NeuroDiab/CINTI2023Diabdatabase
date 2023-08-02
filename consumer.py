#!/usr/bin/python3
from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
import time
import json
import pymongo
from dotenv import load_dotenv
import os

load_dotenv()

c = None
myclient = None

MongoConnection = os.getenv("MONGOCONNECT") + "?replicaSet=" + os.getenv("MONGOREPLICA") + "&authSource=admin"

conf = {
                    'bootstrap.servers': os.getenv("SERVERS"),
                    'security.protocol': 'SASL_SSL',
                    'sasl.mechanisms': 'SCRAM-SHA-512',
                    'key.deserializer': StringDeserializer('utf_8'),
                    'value.deserializer': StringDeserializer('utf_8'),
                    'sasl.username': os.getenv("SASLUSERNAME"),
                    'sasl.password': os.getenv("SASLPASSWORD"),
                    'enable.auto.commit': 'True',
                    'auto.offset.reset': 'earliest',
                    'group.id': os.getenv("GROUPID")
                }

for x in range(0, 10):
    try:
        c = DeserializingConsumer(conf)
        c.subscribe([os.getenv("TOPICNAME")])
        print("Kafka consumer is ready!")

        myclient = pymongo.MongoClient(MongoConnection)

        print("MongoDB is ready!")
        break
    except:
        print("Error in kafka")
        time.sleep(5)

if __name__ == '__main__':
    uploadBatch = []
    colname = ""
    while True:
        msg = c.poll(5.0)
        if msg is None:
            if uploadBatch:
                myclient.diabetes_database[colname].insert_many(uploadBatch)
                uploadBatch = []
            else:
                continue
        elif msg.error():
            print('Error: {}'.format(msg.error()))
            continue
        else:
            print(msg.partition())
            if uploadBatch:
                if msg.key() == colname:
                    uploadBatch.append(json.loads(msg.value()))
                else:
                    myclient.diabetes_database[colname].insert_many(uploadBatch)
                    uploadBatch = []
                    colname = msg.key()
                    uploadBatch.append(json.loads(msg.value()))
            else:
                colname = msg.key()
                uploadBatch.append(json.loads(msg.value()))

        if len(uploadBatch) == 10000:
            myclient.diabetes_database[colname].insert_many(uploadBatch)
            uploadBatch = []


    c.close()