from datetime import datetime
from collections import defaultdict
import numpy as np
import pandas as pd
from pymongo import MongoClient
import matplotlib.pyplot as plt
from statsmodels.tsa.stattools import adfuller
import matplotlib.dates as mdates

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

# Convert 'hours_since_start' to a datetime index starting from 'start_date'
df_completed['datetime'] = pd.to_datetime(df_completed['hours_since_start'], unit='h', origin=start_date)

# Perform the ADF test
adf_test_filled = adfuller(df_completed['count'])

# Output the ADF statistic and p-value
print(f"ADF Statistic: {adf_test_filled[0]}")
print(f"p-value: {adf_test_filled[1]}")
for key, value in adf_test_filled[4].items():
    print(f"   {key}, {value}")

# Check the stationarity of the filled series
if adf_test_filled[1] < 0.05:
    print("The filled time series is stationary.")
else:
    print("The filled time series is not stationary.")

# Convert 'count' to a series for rolling operations
counts_series_filled = df_completed['count']

# Calculate the rolling mean and the rolling standard deviation
rolling_mean_filled = counts_series_filled.rolling(window=24).mean()
rolling_std_filled = counts_series_filled.rolling(window=24).std()

# Plot the filled time series with rolling statistics
fig, ax = plt.subplots(figsize=(14, 7))
ax.plot(counts_series_filled, color='blue', label='Filled Series')
ax.plot(rolling_mean_filled, color='red', label='Rolling Mean')
ax.plot(rolling_std_filled, color='black', label='Rolling Std Dev')

ax.set_title('Filled Time Series with Rolling Mean & Standard Deviation')
ax.set_xlabel('Hour')
ax.set_ylabel('Counts')

# To improve readability of the x-axis
plt.xticks(rotation=45)
ax.xaxis.set_major_locator(plt.MaxNLocator(8))

ax.legend()
plt.show()