from adls.adls import AzureDataLake
from consumers.fluid import fluid_consumer
from generators.fluid_quality_generator import fluid_quality_generator
import threading

threading.Thread(target=fluid_consumer, args=[['localhost:9092'], 'fluid']).start()
threading.Thread(target=fluid_quality_generator, args=[['localhost:9092']]).start()

