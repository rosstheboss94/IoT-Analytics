from producers.fluid import fluid_producer
import matplotlib.pyplot as plt
import numpy as np
import random
import struct
import time 

# Generate synthetic oil or fluid quality data
def generate_oil_quality_data(initial_quality, num_samples, anomaly_index, anomaly_factor):
    quality_data = [initial_quality]

    for i in range(1, num_samples):
        if i == anomaly_index:
            quality_data.append(quality_data[i - 1] * anomaly_factor)
        else:
            quality_data.append(quality_data[i - 1] + random.uniform(-2.0, 2.0))

    return quality_data

def fluid_quality_generator(bootstrap_server, initial_quality=50.0, num_samples=7200, time_period=1.0, sampling_rate=100):
    # Introduce an anomaly (drop in oil quality)
    anomaly_index = random.randint(100, num_samples - 100)
    anomaly_factor = 0.5
    oil_quality_data = generate_oil_quality_data(initial_quality, num_samples, anomaly_index, anomaly_factor)
    
    for data_point in oil_quality_data:
        data_point_bytes = struct.pack('f', round(data_point, 2))
        fluid_producer(bootstrap_server, 'fluid', data_point_bytes)
        time.sleep(1)

