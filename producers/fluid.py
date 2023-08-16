from kafka import KafkaProducer
import socket
import struct
import time


def fluid_producer(servers, topic, value):  
    try:
        producer = KafkaProducer(bootstrap_servers=servers) 
        
        producer.send(topic, value)
            
    except Exception as e:
        print(e)
