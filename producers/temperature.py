from kafka import KafkaProducer

def temperature_producer(servers, topic, value):
    producer = KafkaProducer(bootstrap_servers=servers)

    producer.send(topic, value)