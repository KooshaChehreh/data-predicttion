from datetime import datetime
from collections import defaultdict
import numpy as np
import pandas as pd
from pymongo import MongoClient
import matplotlib.pyplot as plt
from statsmodels.tsa.stattools import adfuller
import matplotlib.dates as mdates
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf

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

# Create a standard dictionary to hold the counts for each day and hour
counts_dict = {}

# Populate the dictionary with the query results
for record in records:
    day = record['_id']['day']
    hour = record['_id']['hour']
    count = record['count']
    
    if day not in counts_dict:
        counts_dict[day] = {}  # Create a new dictionary for the day if it doesn't exist
    
    # Assuming that there will be only one count per day and hour,
    # otherwise, you need logic to sum or handle multiple counts
    counts_dict[day][hour] = count


# Initialize an empty dictionary to hold lists of counts for each hour
hourly_averages = {}

# Populate the dictionary with counts for each hour
for day, hours in counts_dict.items():
    for hour, count in hours.items():
        if hour not in hourly_averages:
            hourly_averages[hour] = []  # Initialize a new list if the hour key doesn't exist
        hourly_averages[hour].append(count)

# Compute averages using numpy for mean calculation
hourly_averages = {hour: round(np.mean(counts), 2) for hour, counts in hourly_averages.items()}
# Fill in missing hours for each day with the average counts
completed_counts = []
for day in range(1, 32):  # Assuming December has 31 days

    for hour in range(24):

        # Use get to provide a default empty dictionary if the day is not in counts_dict
        # The method get is a function available on Python dictionaries that allows you to 
        # retrieve the value for a given key if the key is present in the dictionary. 
        # If the key is not present, instead of raising a KeyError, the get function 
        # returns a default value that you specify

        hours_dict = counts_dict.get(day, {})
        
        # Use get to provide the hourly average as a default if the hour is not in hours_dict
        count = hours_dict.get(hour, hourly_averages.get(hour, 0))
        
        completed_counts.append(((day - 1) * 24 + hour, count))

# Convert completed_counts into a DataFrame
df_completed = pd.DataFrame(completed_counts, columns=['hours_since_start', 'count'])


# Assuming 'y' is the column in 'df' DataFrame containing your time series data
time_series = df_completed['count']

# Plot ACF
plt.figure()
plot_acf(time_series, lags=40, alpha=0.05)  # Adjust lags as needed
plt.title('Autocorrelation Function')
plt.show()

# Plot PACF
plt.figure()
plot_pacf(time_series, lags=40, alpha=0.05, method='ywm')  # Adjust lags and method as needed
plt.title('Partial Autocorrelation Function')
plt.show()