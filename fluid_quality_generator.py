import numpy as np
import matplotlib.pyplot as plt
import random
from producers.fluid import produce
import struct
import time 

# Parameters for the synthetic data
num_samples = 1000
time_period = 1.0  # Time period in seconds
sampling_rate = 100  # Sampling rate in Hz
initial_quality = 50.0  # Initial oil or fluid quality

# Generate time values
time_values = np.linspace(0, time_period, num_samples)

# Generate synthetic oil or fluid quality data
def generate_oil_quality_data(initial_quality, num_samples, anomaly_index, anomaly_factor):
    quality_data = [initial_quality]
    for i in range(1, num_samples):
        if i == anomaly_index:
            quality_data.append(quality_data[i - 1] * anomaly_factor)
        else:
            quality_data.append(quality_data[i - 1] + random.uniform(-2.0, 2.0))
    return quality_data

# Introduce an anomaly (drop in oil quality)
anomaly_index = random.randint(100, num_samples - 100)
anomaly_factor = 0.5
oil_quality_data = generate_oil_quality_data(initial_quality, num_samples, anomaly_index, anomaly_factor)

for data_point in oil_quality_data:
    data_point_bytes = struct.pack('f', round(data_point, 2))
    produce(['localhost:9092'], 'fluid', data_point_bytes)
    time.sleep(1)
# print(oil_quality_data)

# Plot the synthetic oil quality data
# plt.plot(time_values, oil_quality_data)
# plt.title("Synthetic Oil Quality Sensor Data")
# plt.xlabel("Time (s)")
# plt.ylabel("Oil Quality")
# plt.show()
