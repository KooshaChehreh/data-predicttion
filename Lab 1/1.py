from datetime import datetime
import numpy as np
import pandas as pd
from pymongo import MongoClient
import matplotlib.pyplot as plt


client = MongoClient("mongodb://ictts:Ict4SM22!@bigdatadb.polito.it:27017/carsharing?ssl=true&authSource=carsharing&tlsAllowInvalidCertificates=true")

# Get the database
db = client['carsharing']


#ّ Function to derive CDF
def plot_cdf(data, xlabel="X-axis Label", ylabel="CDF", title="Cumulative Distribution Function"):
    sorted_data = np.sort(data)
    yvals = np.arange(len(sorted_data)) / float(len(sorted_data))

    plt.plot(sorted_data, yvals, marker='.', linestyle='-', color='b')
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(True)
    plt.show()

# Q1 --> MongoDB query - Bookings
start_date = datetime(2017, 11, 1)
end_date = datetime(2018, 1, 31)

documents_torino = db["PermanentBookings"].find({
    "city": "Torino",
    "init_date": {
        "$gte": start_date,
        "$lte": end_date
    }})
documents_stuttgart = db["PermanentBookings"].find({
    "city" : "Stuttgart",
    "init_date": {
        "$gte": start_date,
        "$lte": end_date
    }})
documents_columbus = db["PermanentBookings"].find({
    "city" : "Columbus",
    "init_date": {
        "$gte": start_date,
        "$lte": end_date
    }})


# Calculate durations of Torino
durations_torino = []
for doc in documents_torino:
    init_date = doc['init_date']
    final_date = doc['final_date']
    duration = (final_date - init_date).total_seconds()
    durations_torino.append(duration)

plot_cdf(durations_torino, xlabel="Duration", ylabel="CDF", title="Torino Cumulative Distribution Function of Durations Bookings")


# Calculate durations of stuttgart
durations_stuttgart = []
for doc in documents_stuttgart:
    init_date = doc['init_date']
    final_date = doc['final_date']
    duration = (final_date - init_date).total_seconds()
    durations_stuttgart.append(duration)

plot_cdf(durations_stuttgart, xlabel="Duration", ylabel="CDF", title="Stuttgart Cumulative Distribution Function of Durations Bookings")

# Calculate durations of columbus
durations_columbus = []
for doc in documents_columbus:
    init_date = doc['init_date']
    final_date = doc['final_date']
    duration = (final_date - init_date).total_seconds()
    durations_columbus.append(duration)

plot_cdf(durations_columbus, xlabel="Duration", ylabel="CDF", title="Columbus Cumulative Distribution Function of Durations Bookings")


# Q1 --> MongoDB query - Parkings
start_date = datetime(2017, 11, 1)
end_date = datetime(2018, 1, 31)

documents_torino_parking = db["PermanentParkings"].find({
    "city": "Torino",
    "init_date": {
        "$gte": start_date,
        "$lte": end_date
    }})
documents_stuttgart_parking  = db["PermanentParkings"].find({
    "city" : "Stuttgart",
    "init_date": {
        "$gte": start_date,
        "$lte": end_date
    }})
documents_columbus_parking  = db["PermanentParkings"].find({
    "city" : "Columbus",
    "init_date": {
        "$gte": start_date,
        "$lte": end_date
    }})


# Calculate durations of Torino
durations_torino_parking  = []
for doc in documents_torino_parking:
    init_date = doc['init_date']
    final_date = doc['final_date']
    duration = (final_date - init_date).total_seconds()
    durations_torino_parking.append(duration)

plot_cdf(durations_torino_parking, xlabel="Duration", ylabel="CDF", title="Torino Cumulative Distribution Function of Durations Parking")


# Calculate durations of stuttgart
durations_stuttgart_parking  = []
for doc in documents_stuttgart_parking:
    init_date = doc['init_date']
    final_date = doc['final_date']
    duration = (final_date - init_date).total_seconds()
    durations_stuttgart_parking.append(duration)

plot_cdf(durations_stuttgart_parking, xlabel="Duration", ylabel="CDF", title="Stuttgart Cumulative Distribution Function of Durations Parking")

# Calculate durations of columbus
durations_columbus_parking  = []
for doc in documents_columbus_parking:
    init_date = doc['init_date']
    final_date = doc['final_date']
    duration = (final_date - init_date).total_seconds()
    durations_columbus_parking.append(duration)
 
plot_cdf(durations_columbus_parking, xlabel="Duration", ylabel="CDF", title="Columbus Cumulative Distribution Function of Durations Parking")


# Q1 - Part A --> Needs explanation according to previuos part plots

# Q1 - Part B --> Needs explanation according to previuos part plots

# Q1 - Part C:

# Torino
data_torino = []
for doc in documents_torino:
    init_date = pd.to_datetime(doc['init_date'])
    final_date = pd.to_datetime(doc['final_date'])
    duration = (final_date - init_date).total_seconds()
    week = init_date.week
    data_torino.append((week, duration))

# Convert to DataFrame for easier manipulation
df = pd.DataFrame(data_torino, columns=['week', 'duration'])

# Group data by week and calculate CDF for each week
grouped_data = df.groupby('week')['duration']

# Plot CDF for each week
plt.figure(figsize=(10, 6))
for name, group in grouped_data:
    group.hist(cumulative=True, density=1, bins=100, label=f'Week {name}', histtype='step', linewidth=1.5)
    
plt.title('CDF of Booking/Parking Durations by Week - Torino')
plt.xlabel('Duration (s)')
plt.ylabel('CDF')
plt.legend()
plt.grid(True)
plt.show()


# Stuttgart

data_stuttgart = []
for doc in documents_stuttgart:
    init_date = pd.to_datetime(doc['init_date'])
    final_date = pd.to_datetime(doc['final_date'])
    duration = (final_date - init_date).total_seconds()
    week = init_date.week
    data_stuttgart.append((week, duration))

# Convert to DataFrame for easier manipulation
df = pd.DataFrame(data_stuttgart, columns=['week', 'duration'])

# Group data by week and calculate CDF for each week
grouped_data = df.groupby('week')['duration']

# Plot CDF for each week
plt.figure(figsize=(10, 6))
for name, group in grouped_data:
    group.hist(cumulative=True, density=1, bins=100, label=f'Week {name}', histtype='step', linewidth=1.5)
    
plt.title('CDF of Booking/Parking Durations by Week - Stuttgart')
plt.xlabel('Duration (s)')
plt.ylabel('CDF')
plt.legend()
plt.grid(True)
plt.show()


# Stuttgart

data_columbus = []
for doc in documents_columbus:
    init_date = pd.to_datetime(doc['init_date'])
    final_date = pd.to_datetime(doc['final_date'])
    duration = (final_date - init_date).total_seconds()
    week = init_date.week
    data_columbus.append((week, duration))

# Convert to DataFrame for easier manipulation
df = pd.DataFrame(data_columbus, columns=['week', 'duration'])

# Group data by week and calculate CDF for each week
grouped_data = df.groupby('week')['duration']

# Plot CDF for each week
plt.figure(figsize=(10, 6))
for name, group in grouped_data:
    group.hist(cumulative=True, density=1, bins=100, label=f'Week {name}', histtype='step', linewidth=1.5)
    
plt.title('CDF of Booking/Parking Durations by Week - Columbus')
plt.xlabel('Duration (s)')
plt.ylabel('CDF')
plt.legend()
plt.grid(True)
plt.show()
