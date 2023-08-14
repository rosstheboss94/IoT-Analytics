from kafka import KafkaProducer

def fluid_producer(servers, topic, value):
    producer = KafkaProducer(bootstrap_servers=servers)

    producer.send(topic, value)