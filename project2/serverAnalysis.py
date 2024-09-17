# import pandas as pd

# # Load the parquet files
# serversX = pd.read_parquet('servers4.parquet')

# # Filter the data to include only the relevant src_ip and dst_ip pairs
# public_server_ips = ['200.0.0.12', '200.0.0.11']
# filtered_data = serversX[serversX['dst_ip'].isin(public_server_ips)]

# # Calculate the down_bytes / up_bytes ratio
# filtered_data['bytes_ratio'] = filtered_data['down_bytes'] / filtered_data['up_bytes']

# # Group by src_ip and dst_ip and count the occurrences
# flow_counts = filtered_data.groupby(['src_ip', 'dst_ip']).size().reset_index(name='count')

# # Pivot the data for better readability
# flow_counts_pivot = flow_counts.pivot(index='src_ip', columns='dst_ip', values='count').fillna(0).astype(int)

# # Calculate the mean of the bytes ratio for each src_ip
# mean_bytes_ratio = filtered_data.groupby('src_ip')['bytes_ratio'].mean().reset_index()

# # Merge the flow counts and mean bytes ratio data
# result = flow_counts_pivot.merge(mean_bytes_ratio, on='src_ip')

# # Print the result
# print(result)

# # Save to a CSV if needed
# result.to_csv('src_ip_to_dst_ip_flow_counts_and_ratios.csv')

import pandas as pd

# Step 1: Read data from Parquet file
df = pd.read_parquet('servers4.parquet')

# Step 2: Sort DataFrame by timestamp (if not already sorted)
df = df.sort_values(by='timestamp')

# Step 3: Calculate time intervals between consecutive flows for each src_ip
df['time_diff'] = df.groupby('src_ip')['timestamp'].diff()

# Step 4: Identify anomalies based on time intervals
# Let's define anomalies as intervals greater than a threshold (e.g., 10 seconds)
anomalies = df[df['time_diff'].fillna(0) > 10]

# Step 5: Print or further analyze anomalies
print("Anomalies based on timestamp:")
print(anomalies[['timestamp', 'src_ip', 'time_diff']])

# Step 6: Get time_diff values that are exactly 0
zero_time_diff = df[df['time_diff'] == 0]

# Step 7: Print or further analyze time_diff values that are exactly 0
print("\nFlows with time_diff = 0:")
print(zero_time_diff[['timestamp', 'src_ip', 'time_diff']])

# Step 8: Extract unique src_ips from flows with time_diff = 0
unique_src_ips_with_zero_diff = zero_time_diff['src_ip'].unique()

# Step 9: Count the number of unique src_ips with time_diff = 0
num_unique_src_ips_with_zero_diff = len(unique_src_ips_with_zero_diff)

# Step 10: Print the count of unique src_ips with time_diff = 0
print("\nSrc IPs with time_diff = 0:")
print(unique_src_ips_with_zero_diff)
print("\nNumber of unique Src IPs with time_diff = 0:", num_unique_src_ips_with_zero_diff)
