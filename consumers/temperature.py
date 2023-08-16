from adls.adls import AzureDataLake
from kafka import KafkaConsumer
import struct
import datetime
import time

def temperature_consumer(servers, topic):
    consumer = KafkaConsumer(topic, bootstrap_servers=servers, value_deserializer=lambda x: struct.unpack('f',x)[0])
    sas_token = '?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwlacupx&se=2024-08-13T12:18:04Z&st=2023-08-13T04:18:04Z&spr=https&sig=zgIyaOIIgS%2F8M6FYGmOY6eBJeDcaOJ6LgE9gNemWgSM%3D'
    hour_messages = []
    hour = datetime.datetime.now().hour
    
    for i, message in enumerate(consumer):
        message_time = datetime.datetime.fromtimestamp(message.timestamp / 1000)
        
        print('temperature consumer ', message.value)
        
        if message_time.hour == hour:
            hour_messages.append({
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "timestamp": message.timestamp,
                "key": str(message.key),
                "value": message.value,
                "headers": message.headers
            })
        else:
            iot_lake = AzureDataLake('iotanalytics1994', sas_token)
            file_system = iot_lake.create_file_system('temperature')
            directory = iot_lake.create_directory(file_system, f'year={message_time.year}/month={message_time.month}/day={message_time.day}/hour={hour}')
            iot_lake.upload_json_file_to_directory(directory, f'temperature{message_time.year}{message_time.month}{message_time.day}_{hour}.json', hour_messages)
            hour_messages = []
            hour = message_time.hour
        
        time.sleep(.01)