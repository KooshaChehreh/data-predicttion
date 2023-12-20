from datetime import datetime, timedelta
from collections import defaultdict
import numpy as np
import pandas as pd
from pymongo import MongoClient
import matplotlib.pyplot as plt

client = MongoClient("mongodb://ictts:Ict4SM22!@bigdatadb.polito.it:27017/carsharing?ssl=true&authSource=carsharing&tlsAllowInvalidCertificates=true")

# Get the database
db = client['carsharing']

# Define the start and end dates for the query
start_date = datetime(2017, 12, 1)
end_date = datetime(2018, 1, 1)

# Define the query conditions
query_conditions = {
    'init_date': {'$gte': start_date, '$lt': end_date},
    'city': "Torino",
    '$expr': {'$lt': [{'$subtract': ['$final_time', '$init_time']}, 8000]},
    '$expr': {'$ne': ['$init_address', '$final_address']}
}

# Aggregation pipeline
pipeline = [
    {
        '$match': query_conditions
    },
    {
        '$group': {
            '_id': {
                'day': {'$dayOfMonth': '$init_date'},
                'hour': {'$hour': '$init_date'}
            },
            'count': {'$sum': 1}
        }
    },
    {
        '$sort': {
            '_id.day': 1,
            '_id.hour': 1
        }
    }
]

# Execute the query to retrieve the records
records = list(db["PermanentBookings"].aggregate(pipeline))

# Create a dictionary to hold the counts for each day and hour
counts_dict = defaultdict(lambda: defaultdict(int))

# Populate the dictionary with the query results
for record in records:
    day = record['_id']['day']
    hour = record['_id']['hour']
    count = record['count']
    counts_dict[day][hour] = count

# Calculate the average count for each hour across all days
hourly_averages = defaultdict(list)
for day, hours in counts_dict.items():
    for hour, count in hours.items():
        hourly_averages[hour].append(count)

# Compute averages
hourly_averages = {hour: np.mean(counts) for hour, counts in hourly_averages.items()}

# Fill in missing hours for each day with the average counts
completed_counts = []
for day in range(1, 32):  # Assuming December has 31 days
    for hour in range(24):
        if hour in counts_dict[day]:
            completed_counts.append(((day - 1) * 24 + hour, counts_dict[day][hour]))
        else:
            completed_counts.append(((day - 1) * 24 + hour, hourly_averages[hour]))

# Unzip the completed_counts for plotting
hours, counts = zip(*completed_counts)

# Plotting
plt.figure(figsize=(15, 6))
plt.plot(hours, counts, color='blue', linestyle='-', linewidth="2", marker='', label='Bookings Count')

# Label formatting to show day and hour
tick_positions = range(0, 24 * 31, 24)  # One tick per day
tick_labels = [f'Day {i // 24 + 1}' for i in tick_positions]
plt.xticks(tick_positions, tick_labels, rotation=45)  # Set x-ticks to be every day and rotate for readability

plt.xlabel('Day and Hour')
plt.ylabel('Count of Bookings')
plt.title('Count of Bookings for Each Hour of December 2017')
plt.grid(True)  # Add grid for better readability
plt.legend()

# Show the plot
plt.tight_layout()  # Adjust layout so everything fits without overlapping
plt.show()