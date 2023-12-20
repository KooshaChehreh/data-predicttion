from datetime import datetime, timedelta
from collections import defaultdict
import numpy as np
import pandas as pd
from pymongo import MongoClient
import matplotlib.pyplot as plt
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_squared_error

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


# Ensure that the DataFrame index is a DateTimeIndex
if not isinstance(df_completed.index, pd.DatetimeIndex):
    df_completed.index = pd.to_datetime([datetime(2017, 12, 1) + timedelta(hours=x) for x in df_completed.index])

# Define the training and testing period
train_end = pd.Timestamp('2017-12-15 00:00:00')
test_end = pd.Timestamp('2017-12-31 00:00:00')

# Split the data into train and test sets
train_data = df_completed.loc[df_completed.index < train_end, 'count']
test_data = df_completed.loc[(df_completed.index >= train_end) & (df_completed.index < test_end), 'count']

print(df_completed)
# Fit the ARIMA model on the training set
model = ARIMA(train_data, order=(20, 1, 15))
fitted_model = model.fit()

# Predict on the test set
# The 'start' should be the index of the first element in test_data
# The 'end' should be the index of the last element in test_data
start = test_data.index[0]
end = test_data.index[-1]

# Make predictions
predictions = fitted_model.predict(start=start, end=end, typ='levels')

# Calculate the mean squared error
mse = mean_squared_error(test_data, predictions)
print(f'The Mean Squared Error of our forecasts is {mse}')

# Plot the results
plt.figure(figsize=(10, 6))
plt.plot(train_data, label='Train Data')
plt.plot(test_data, label='Actual Test Data')
plt.plot(test_data.index, predictions, label='Predicted Test Data', linestyle='--')
plt.legend(loc='best')
plt.title('Train/Test Split for Time Series Data')
plt.show()