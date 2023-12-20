from datetime import datetime
from pymongo import MongoClient
import matplotlib.pyplot as plt
import pandas as pd

client = MongoClient("mongodb://ictts:Ict4SM22!@bigdatadb.polito.it:27017/carsharing?ssl=true&authSource=carsharing&tlsAllowInvalidCertificates=true")

# Get the database
db = client['carsharing']
start_date = datetime(2017, 11, 1)
end_date = datetime(2018, 1, 31)

""" Torino """

# MongoDB query for ActiveBookings
hours_booking_active_bookings = db["PermanentBookings"].aggregate([
    {
        "$match": {
            "city": {
                "$in": ["Torino"]
            },
            "init_date": {
        "$gte": start_date,
        "$lte": end_date
    }
        }
    },
    {
        "$project": {
            "hourOfDay": { "$hour": "$init_date" },
            "city": 1
        }
    },
    {
        "$group": {
            "_id": {
                "hourOfDay": "$hourOfDay",
                "city": "$city"
            },
            "count": { "$sum": 1 }
        }
    },
    {
        "$sort": {
            "_id.hourOfDay": 1
        }
    }
])

# MongoDB query for ActiveParkings
hours_booking_active_parkings = db["PermanentParkings"].aggregate([
    {
        "$match": {
            "city": {
                "$in": ["Torino"]
            },
            "init_date": {
        "$gte": start_date,
        "$lte": end_date
    }
        }
    },
    {
        "$project": {
            "hourOfDay": { "$hour": "$init_date" },
            "city": 1
        }
    },
    {
        "$group": {
            "_id": {
                "hourOfDay": "$hourOfDay",
                "city": "$city"
            },
            "count": { "$sum": 1 }
        }
    },
    {
        "$sort": {
            "_id.hourOfDay": 1
        }
    }
])

# Convert MongoDB output from Mongo object to list
hours_booking_active_bookings = list(hours_booking_active_bookings)
hours_booking_active_parkings = list(hours_booking_active_parkings)

# Convert MongoDB output to list of dictionaries with list comprehension
data_active_bookings = [
    {'hourOfDay': doc['_id']['hourOfDay'], 'count': doc['count'], 'type': 'ActiveBookings'} 
    for doc in hours_booking_active_bookings
]

data_active_parkings = [
    {'hourOfDay': doc['_id']['hourOfDay'], 'count': doc['count'], 'type': 'ActiveParkings'} 
    for doc in hours_booking_active_parkings
]

# Combine the data to plot these two cdf in one chart
combined_data = data_active_bookings + data_active_parkings

# Convert list of dictionaries to DataFrame
df = pd.DataFrame(combined_data)

# Pivot DataFrame to get hours as index and types as columns
pivot_df = df.pivot(index='hourOfDay', columns='type', values='count')

# Fill NaN values with 0
pivot_df.fillna(0, inplace=True)

# Calculate cumulative sum
pivot_df = pivot_df.cumsum()

# Plot data
plt.figure(figsize=(10, 6))
for column in pivot_df.columns:
    plt.plot(pivot_df.index, pivot_df[column], marker='', linewidth=2, label=column)
plt.legend(loc='best')
plt.title('Cumulative Hourly Counts In Torino')
plt.xlabel('Hour of Day')
plt.ylabel('Cumulative Count')
plt.grid(True)
plt.show()



""" Stuttgart  """
# MongoDB query for ActiveBookings
hours_booking_active_bookings = db["PermanentBookings"].aggregate([
    {
        "$match": {
            "city": {
                "$in": ["Stuttgart"]
            },
            "init_date": {
        "$gte": start_date,
        "$lte": end_date
    }
        }
    },
    {
        "$project": {
            "hourOfDay": { "$hour": "$init_date" },
            "city": 1
        }
    },
    {
        "$group": {
            "_id": {
                "hourOfDay": "$hourOfDay",
                "city": "$city"
            },
            "count": { "$sum": 1 }
        }
    },
    {
        "$sort": {
            "_id.hourOfDay": 1
        }
    }
])

# MongoDB query for ActiveParkings
hours_booking_active_parkings = db["PermanentParkings"].aggregate([
    {
        "$match": {
            "city": {
                "$in": ["Stuttgart"]
            },
            "init_date": {
        "$gte": start_date,
        "$lte": end_date
    }
        }
    },
    {
        "$project": {
            "hourOfDay": { "$hour": "$init_date" },
            "city": 1
        }
    },
    {
        "$group": {
            "_id": {
                "hourOfDay": "$hourOfDay",
                "city": "$city"
            },
            "count": { "$sum": 1 }
        }
    },
    {
        "$sort": {
            "_id.hourOfDay": 1
        }
    }
])

# Convert MongoDB output to list
hours_booking_active_bookings = list(hours_booking_active_bookings)
hours_booking_active_parkings = list(hours_booking_active_parkings)

# Convert MongoDB output to list of dictionaries
data_active_bookings = [
    {'hourOfDay': doc['_id']['hourOfDay'], 'count': doc['count'], 'type': 'ActiveBookings'} 
    for doc in hours_booking_active_bookings
]

data_active_parkings = [
    {'hourOfDay': doc['_id']['hourOfDay'], 'count': doc['count'], 'type': 'ActiveParkings'} 
    for doc in hours_booking_active_parkings
]

# Combine the data
combined_data = data_active_bookings + data_active_parkings

# Convert list of dictionaries to DataFrame
df = pd.DataFrame(combined_data)

# Pivot DataFrame to get hours as index and types as columns
pivot_df = df.pivot(index='hourOfDay', columns='type', values='count')

# Fill NaN values with 0
pivot_df.fillna(0, inplace=True)

# Calculate cumulative sum
pivot_df = pivot_df.cumsum()

# Plot data
plt.figure(figsize=(10, 6))
for column in pivot_df.columns:
    plt.plot(pivot_df.index, pivot_df[column], marker='', linewidth=2, label=column)
plt.legend(loc='best')
plt.title('Cumulative Hourly Counts In Stuttgart')
plt.xlabel('Hour of Day')
plt.ylabel('Cumulative Count')
plt.grid(True)
plt.show()




""" Columbus  """
# MongoDB query for ActiveBookings
hours_booking_active_bookings = db["PermanentBookings"].aggregate([
    {
        "$match": {
            "city": {
                "$in": ["Columbus"]
            },
            "init_date": {
        "$gte": start_date,
        "$lte": end_date
    }
        }
    },
    {
        "$project": {
            "hourOfDay": { "$hour": "$init_date" },
            "city": 1
        }
    },
    {
        "$group": {
            "_id": {
                "hourOfDay": "$hourOfDay",
                "city": "$city"
            },
            "count": { "$sum": 1 }
        }
    },
    {
        "$sort": {
            "_id.hourOfDay": 1
        }
    }
])

# MongoDB query for ActiveParkings
hours_booking_active_parkings = db["PermanentParkings"].aggregate([
    {
        "$match": {
            "city": {
                "$in": ["Columbus"]
            },
            "init_date": {
        "$gte": start_date,
        "$lte": end_date
    }
        }
    },
    {
        "$project": {
            "hourOfDay": { "$hour": "$init_date" },
            "city": 1
        }
    },
    {
        "$group": {
            "_id": {
                "hourOfDay": "$hourOfDay",
                "city": "$city"
            },
            "count": { "$sum": 1 }
        }
    },
    {
        "$sort": {
            "_id.hourOfDay": 1
        }
    }
])

# Convert MongoDB output to list
hours_booking_active_bookings = list(hours_booking_active_bookings)
hours_booking_active_parkings = list(hours_booking_active_parkings)

# Convert MongoDB output to list of dictionaries
data_active_bookings = [
    {'hourOfDay': doc['_id']['hourOfDay'], 'count': doc['count'], 'type': 'ActiveBookings'} 
    for doc in hours_booking_active_bookings
]

data_active_parkings = [
    {'hourOfDay': doc['_id']['hourOfDay'], 'count': doc['count'], 'type': 'ActiveParkings'} 
    for doc in hours_booking_active_parkings
]

# Combine the data
combined_data = data_active_bookings + data_active_parkings

# Convert list of dictionaries to DataFrame
df = pd.DataFrame(combined_data)

# Pivot DataFrame to get hours as index and types as columns
pivot_df = df.pivot(index='hourOfDay', columns='type', values='count')

# Fill NaN values with 0
pivot_df.fillna(0, inplace=True)

# Calculate cumulative sum
pivot_df = pivot_df.cumsum()

# Plot data
plt.figure(figsize=(10, 6))
for column in pivot_df.columns:
    plt.plot(pivot_df.index, pivot_df[column], marker='', linewidth=2, label=column)
plt.legend(loc='best')
plt.title('Cumulative Hourly Counts In Columbus')
plt.xlabel('Hour of Day')
plt.ylabel('Cumulative Count')
plt.grid(True)
plt.show()