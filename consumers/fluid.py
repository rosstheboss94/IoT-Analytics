from kafka import KafkaConsumer
import struct
import datetime

def consume(servers, topic):
    consumer = KafkaConsumer(topic, bootstrap_servers=servers, value_deserializer=lambda x: struct.unpack('f',x)[0])
    

    hour_messages = []
    for i, message in enumerate(consumer):
        
        message_time = datetime.datetime.fromtimestamp(message.timestamp / 1000)
        
        hour_messages.append({
            'topic': message.topic,
            'partition': message.partition,
            'offset': message.offset,
            'timestamp': message.timestamp,
            'key': message.key,
            'value': message.value,
            'headers': message.headers
        })

        

        # print(message.value)

consume(['localhost:9092'], 'fluid')