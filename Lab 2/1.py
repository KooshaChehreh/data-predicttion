from datetime import datetime
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
    '$and': [
        {'init_date': {'$gte': start_date}},
        {'init_date': {'$lt': end_date}},
        {'$expr': {'$lt': [{'$subtract': ['$final_time', '$init_time']}, 8000]}},
        {'$expr': {'$ne': ['$init_address', '$final_address']}}
    ]
}

# Aggregation pipeline
pipeline = [
    {
        '$match': query_conditions,
        "city": "Torino"
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
records = db["PermanentBookings"].aggregate(pipeline)

# Prepare the data for plotting
hours = []
counts = []
for record in records:
    # Convert day and hour to a single hour number for the entire month
    day_hour = (record['_id']['day'] - 1) * 24 + record['_id']['hour']
    hours.append(day_hour)
    counts.append(record['count'])

# Plotting
plt.figure(figsize=(15, 6))

# Connect the points with lines
plt.plot(hours, counts, color='blue', linestyle='-', linewidth="2", marker='', label='Bookings Count')

# Label formatting to show day and hour
tick_positions = range(0, 24 * 30, 24)  # One tick per day
tick_labels = [f'Day {i // 24 + 1}' for i in tick_positions]
plt.xticks(tick_positions, tick_labels, rotation=45)  # Set x-ticks to be every day and rotate for readability

plt.xlabel('Day and Hour')
plt.ylabel('Count of Bookings')
plt.title('Count of Bookings for Each Hour of November 2017')
plt.grid(True)  # Add grid for better readability
plt.legend()

# Show the plot
plt.tight_layout()  # Adjust layout so everything fits without overlapping
plt.show()