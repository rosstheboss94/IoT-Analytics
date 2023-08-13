import numpy as np
import matplotlib.pyplot as plt
import random

# Parameters for the synthetic data
num_samples = 1000
time_period = 1.0  # Time period in seconds
sampling_rate = 100  # Sampling rate in Hz
initial_temperature = 25.0  # Initial temperature in Celsius

# Generate time values
time_values = np.linspace(0, time_period, num_samples)

# Generate synthetic temperature data
def generate_temperature_data(initial_temperature, num_samples, anomaly_index, anomaly_value):
    temperature_data = [initial_temperature]
    for i in range(1, num_samples):
        if i == anomaly_index:
            temperature_data.append(temperature_data[i - 1] + anomaly_value)
        else:
            temperature_data.append(temperature_data[i - 1] + random.uniform(-0.5, 0.5))
    return temperature_data

# Introduce an anomaly (temperature increase)
anomaly_index = random.randint(100, num_samples - 100)
anomaly_value = 5.0
temperature_data = generate_temperature_data(initial_temperature, num_samples, anomaly_index, anomaly_value)

# Plot the synthetic temperature data
plt.plot(time_values, temperature_data)
plt.title("Synthetic Temperature Sensor Data")
plt.xlabel("Time (s)")
plt.ylabel("Temperature (Celsius)")
plt.show()
