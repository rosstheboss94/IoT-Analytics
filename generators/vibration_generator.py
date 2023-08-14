# import numpy as np
# import matplotlib.pyplot as plt
# import random

# # Parameters for the synthetic data
# num_samples = 1000
# time_period = 1.0  # Time period in seconds
# sampling_rate = 100  # Sampling rate in Hz
# frequency = 2.0  # Frequency of the vibration signal in Hz
# amplitude = 0.5  # Amplitude of the vibration signal

# # Generate time values
# time_values = np.linspace(0, time_period, num_samples)
# # Generate a sinusoidal vibration signal
# vibration_signal = amplitude * np.sin(2 * np.pi * frequency * time_values)

# # Introduce anomalies (random spikes)
# num_anomalies = 10
# for _ in range(num_anomalies):
#     anomaly_index = random.randint(0, num_samples - 1)
#     vibration_signal[anomaly_index] += random.uniform(1.5, 3.0)

# # Plot the synthetic vibration data
# plt.plot(time_values, vibration_signal)
# plt.title("Synthetic Vibration Sensor Data")
# plt.xlabel("Time (s)")
# plt.ylabel("Amplitude")
# plt.show()
