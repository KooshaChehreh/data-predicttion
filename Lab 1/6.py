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
pipeline = [
    {
        "$match": {
            "city": "Torino",
            "init_date": {
                "$gte": start_date,
                "$lte": end_date
            }
        }
    },
    {
        "$project": {
            "address": "$final_address",
            "hour": { "$hour": "$final_date" }
        }
    },
    {
        "$group": {
            "_id": {
                "address": "$address",
                "hour": "$hour"
            },
            "count": { "$sum": 1 }
        }
    },
]

db_hour_final_destination = db['PermanentBookings'].aggregate(pipeline)

# # Convert the results to a DataFrame
# df = pd.DataFrame(list(db_hour_final_destination))

# # Extract hour and count from the _id field
# df['hour'] = df['_id'].apply(lambda x: x['hour'])
# df.drop(columns=['_id'], inplace=True)

# # Group by hour and sum the counts
# df = df.groupby('hour').sum().reset_index()

# # Plot the DataFrame as a bar plot
# plt.figure(figsize=(10, 5))
# plt.bar(df['hour'], df['count'])
# plt.title('Number of Cars Returned by Hour')
# plt.xlabel('Hour')
# plt.ylabel('Count')
# plt.xticks(range(24))  # Set x-ticks to be hourly
# plt.show()


# Convert the results to a DataFrame
df = pd.DataFrame(list(db_hour_final_destination))

# Normalize the _id field
df = pd.json_normalize(df['_id']).join(df)

# Drop the _id column
df.drop(columns=['_id'], inplace=True)

# Pivot the DataFrame to get count by hour and address
pivot_df = df.pivot(index='address', columns='hour', values='count')

# Replace NaNs with zeros and convert to integers
pivot_df = pivot_df.fillna(0).astype(int)

# Export the DataFrame to a csv file
pivot_df.to_csv('output.xls', sep='\t')

