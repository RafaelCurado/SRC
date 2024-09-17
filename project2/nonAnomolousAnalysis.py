import pandas as pd
import pygeoip
import ipaddress
import matplotlib.pyplot as plt
import requests


# Initialize the GeoIP databases
gi = pygeoip.GeoIP('./GeoIP.dat')
gi2 = pygeoip.GeoIP('./GeoIPASNum.dat')

def get_geolocation(ip):
    try:
        country_code = gi.country_code_by_addr(ip)
        org = gi2.org_by_addr(ip)
        return f"{country_code}, {org}"
    except Exception as e:
        return "Geolocation not found"
    
    

# Load the parquet files
dataX = pd.read_parquet('data4.parquet')
serversX = pd.read_parquet('servers4.parquet')

# Display the first few rows of each dataframe
print("\n\nDATA --------------------------------------------------------------------------------\n")
print(dataX)
print(dataX.describe())



#################################################################################################################### 
# internal - internal  

# # Define the private IP address ranges
# private_ranges = [
#     ipaddress.ip_network('10.0.0.0/8'),
#     ipaddress.ip_network('172.16.0.0/12'),
#     ipaddress.ip_network('192.168.0.0/16')
# ]

# # Filter the DataFrame to include only private IP addresses
# private_ips = dataX[dataX['dst_ip'].apply(lambda x: any(ipaddress.ip_address(x) in private_range for private_range in private_ranges))]

# # Group the data by destination IP and port, then count occurrences
# private_ip_port_counts = private_ips.groupby(['dst_ip', 'port']).size().reset_index(name='count')

# # Get unique ports for each private server (destination IP)
# private_server_ports = private_ip_port_counts.groupby('dst_ip')['port'].unique().reset_index(name='ports')

# # Group by destination IP to find the top 10 internal servers with the most flow counts
# top_10_private_ips = private_ip_port_counts.groupby('dst_ip').apply(lambda x: x.nlargest(3, 'count')).reset_index(drop=True)

# # Merge the top 10 internal servers data with the ports information
# merged_data = pd.merge(top_10_private_ips, private_server_ports, how='left', on='dst_ip')

# # Print results
# print("\nInternal Servers (Destination IPs in the 192.168 network) with the most flow counts and their used ports:")
# print(merged_data)


# # Sort the data by flow count in descending order
# merged_data_sorted = merged_data.sort_values(by='count', ascending=False)

# Plotting
# plt.figure(figsize=(10, 6))
# for index, row in merged_data_sorted.iterrows():
#     plt.barh(f"{row['dst_ip']} - Ports: {', '.join(map(str, row['ports']))}", row['count'], color='skyblue')
# plt.xlabel('Flow Counts')
# plt.ylabel('Internal Servers (Destination IPs)')
# plt.title('Internal Servers with the Most Flow Counts and Their Used Ports')
# plt.gca().invert_yaxis()  # Invert y-axis to display top-to-bottom
# plt.tight_layout()
#plt.show()


#################################################################################################################### 
# internal - external  

# # Filter the DataFrame to include only external IP addresses
# external_ips = dataX[~dataX['dst_ip'].apply(lambda x: any(ipaddress.ip_address(x) in private_range for private_range in private_ranges))]

# # Group the data by destination IP and count occurrences
# external_ip_counts = external_ips.groupby('dst_ip').size().reset_index(name='count')

# # Get the top 10 external IPs with the most flow counts
# top_10_external_ips = external_ip_counts.nlargest(10, 'count')


# # Get geolocation for each IP
# top_10_external_ips['geolocation'] = top_10_external_ips['dst_ip'].apply(get_geolocation)
# # IP geolocalization
# gi = pygeoip.GeoIP('./GeoIP.dat')

# # Filter the DataFrame to include only external IP addresses
# external_ips = dataX[~dataX['dst_ip'].apply(lambda x: any(ipaddress.ip_address(x) in private_range for private_range in private_ranges))]

# # Group the data by destination IP and count occurrences
# external_ip_counts = external_ips['dst_ip'].value_counts().reset_index()
# external_ip_counts.columns = ['dst_ip', 'count']


# # Print results
# print("\nTop 10 External Servers (Destination IPs not in the private network) with the most flow counts:")
# print(top_10_external_ips)
# # Sort the data by flow count in descending order
# top_10_external_ips_sorted = top_10_external_ips.sort_values(by='count', ascending=False)


# Plotting
# plt.figure(figsize=(12, 8))
# bars = plt.barh(top_10_external_ips_sorted['dst_ip'], top_10_external_ips_sorted['count'], color='skyblue')
# plt.xlabel('Flow Counts')
# plt.ylabel('External Servers (Destination IPs)')
# plt.title('Top 10 External Servers with the Most Flow Counts and Their Geolocation')

# # Add geolocation text on the right side of each bar
# for bar, geolocation in zip(bars, top_10_external_ips_sorted['geolocation']):
#     plt.text(bar.get_width(), bar.get_y() + bar.get_height()/2, f"  {geolocation}", va='center', ha='left')

# plt.gca().invert_yaxis()  # Invert y-axis to display top-to-bottom
# plt.tight_layout()
# plt.show()


#################################################################################################################### 
# external - public

# Define the public IP address range (200.0.0.0/24)
public_server_ips = ipaddress.ip_network('200.0.0.0/24')

# Filter the DataFrame to include only public server IPs
public_servers = serversX[serversX['dst_ip'].apply(lambda x: ipaddress.ip_address(x) in public_server_ips)]

# Group the data by source IP address and count occurrences
src_ip_counts = serversX['src_ip'].value_counts()

# Get the top 10 source IP addresses with the most flow counts
top_10_src_ips = src_ip_counts.head(10)

print("\nTop 10 Source IP Addresses with the Most Flow Counts:")
print(top_10_src_ips)  

# Get geolocation for each source IP address
src_ip_geolocation = {}
for src_ip in top_10_src_ips.index:
    src_ip_geolocation[src_ip] = get_geolocation(src_ip)

# Print the geolocation of each source IP address
print("\nGeolocation of the top 10 Source IP addresses:")
for src_ip, geolocation in src_ip_geolocation.items():
    print(f"{src_ip}: {geolocation}")

print("\nAll Destination IPs in the 200.0.0.0/24 network:")
print(public_servers['dst_ip'].unique())


# Plotting
plt.figure(figsize=(12, 8))
bars = plt.barh(top_10_src_ips.index, top_10_src_ips.values, color='skyblue')
plt.xlabel('Flow Counts')
plt.ylabel('Source IP Addresses')
plt.title('Top 10 Source IP Addresses with the Most Flow Counts and Their Geolocation')

# Add geolocation text on the right side of each bar
for bar, (src_ip, geolocation) in zip(bars, src_ip_geolocation.items()):
    plt.text(bar.get_width(), bar.get_y() + bar.get_height()/2, f"  {geolocation}", va='center', ha='left')

plt.gca().invert_yaxis()  # Invert y-axis to display top-to-bottom
plt.tight_layout()
plt.show()
