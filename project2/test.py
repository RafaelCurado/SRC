import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load the data
data = pd.read_csv('src_ip_to_dst_ip_flow_counts_and_ratios.csv')

# Calculate summary statistics for bytes ratio
mean_ratio = data['bytes_ratio'].mean()
std_ratio = data['bytes_ratio'].std()
print(f'Mean bytes ratio: {mean_ratio}')
print(f'Standard deviation of bytes ratio: {std_ratio}')

# Define thresholds for anomaly detection
upper_ratio_threshold = mean_ratio + 2 * std_ratio
lower_ratio_threshold = mean_ratio - 2 * std_ratio

# Identify anomalies based on bytes ratio
ratio_anomalies = data[(data['bytes_ratio'] > upper_ratio_threshold) | (data['bytes_ratio'] < lower_ratio_threshold)]
print('Anomalies based on bytes ratio:')
print(ratio_anomalies)

# Identify anomalies based on flow counts
flow_anomalies = data[(data['200.0.0.11'] == 0) | (data['200.0.0.12'] == 0)]
print('Anomalies based on flow counts:')
print(flow_anomalies)

# Visualize the distribution of bytes ratio
plt.figure(figsize=(10, 6))
sns.histplot(data['bytes_ratio'], kde=True)
plt.axvline(upper_ratio_threshold, color='r', linestyle='--', label='Upper threshold')
plt.axvline(lower_ratio_threshold, color='r', linestyle='--', label='Lower threshold')
plt.title('Distribution of bytes ratio with anomaly thresholds')
plt.xlabel('Bytes ratio')
plt.ylabel('Frequency')
plt.legend()
plt.show()
