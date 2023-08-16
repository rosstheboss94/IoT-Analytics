from adls.adls import AzureDataLake
from consumers.fluid import fluid_consumer
from producers.fluid import fluid_producer
from consumers.temperature import temperature_consumer
from generators.fluid_quality_generator import fluid_quality_generator
from generators.temperature_generator import temperature_generator
import threading

threading.Thread(target=fluid_consumer, args=[['localhost:9092','localhost:9093','localhost:9094'], 'fluid']).start()
#threading.Thread(target=temperature_consumer, args=[['localhost:9092','localhost:9093','localhost:9094'], 'temperature']).start()
threading.Thread(target=fluid_quality_generator, args=[['localhost:9092','localhost:9093','localhost:9094']]).start()
#threading.Thread(target=temperature_generator, args=[['localhost:9092','localhost:9093','localhost:9094']]).start()

