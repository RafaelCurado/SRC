import pandas as pd
import pygeoip
import ipaddress
import matplotlib.pyplot as plt

# Load the parquet files
dataX = pd.read_parquet('data4.parquet')
testX = pd.read_parquet('test4.parquet')
serversX = pd.read_parquet('servers4.parquet')

# Display the first few rows of each dataframe
print("\n\nDATA --------------------------------------------------------------------------------\n")
print(dataX)
print(dataX.describe())

print("\n\nTEST --------------------------------------------------------------------------------\n")
print(testX)
print(testX.describe())

print("\n\nSERVERS -----------------------------------------------------------------------------\n")

print(serversX)
print(serversX.describe())

# Calculate summary statistics for up_bytes
dataX_up_summary = dataX['up_bytes'].agg(['min', 'max', 'mean']).rename('data')
testX_up_summary = testX['up_bytes'].agg(['min', 'max', 'mean']).rename('test')
serversX_up_summary = serversX['up_bytes'].agg(['min', 'max', 'mean']).rename('servers')

# Combine the summaries into a single DataFrame for up_bytes
up_summary_combined = pd.concat([dataX_up_summary, testX_up_summary, serversX_up_summary], axis=1)

# Calculate summary statistics for down_bytes
dataX_down_summary = dataX['down_bytes'].agg(['min', 'max', 'mean']).rename('data')
testX_down_summary = testX['down_bytes'].agg(['min', 'max', 'mean']).rename('test')
serversX_down_summary = serversX['down_bytes'].agg(['min', 'max', 'mean']).rename('servers')

# Combine the summaries into a single DataFrame for down_bytes
down_summary_combined = pd.concat([dataX_down_summary, testX_down_summary, serversX_down_summary], axis=1)

# Disable scientific notation for all dataframes
pd.set_option('display.float_format', lambda x: '%.3f' % x)

# Display the combined summary for up_bytes
print("\n\nSummary of up_bytes for data, test, and servers:\n")
print(up_summary_combined)

# Display the combined summary for down_bytes
print("\n\nSummary of down_bytes for data, test, and servers:\n")
print(down_summary_combined)



# Get unique ports used in each dataset
dataX_ports = dataX['port'].unique()
testX_ports = testX['port'].unique()
serversX_ports = serversX['port'].unique()
print("\n\nUnique ports used in the data dataset:", dataX_ports)
print("Unique ports used in the test dataset:", testX_ports)
print("Unique ports used in the servers dataset:", serversX_ports)



