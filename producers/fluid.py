from kafka import KafkaProducer

def produce(servers, topic, value):
    producer = KafkaProducer(bootstrap_servers=servers)

    producer.send(topic, value)