from datetime import datetime
import numpy as np
import pandas as pd
from pymongo import MongoClient
import matplotlib.pyplot as plt


client = MongoClient("mongodb://ictts:Ict4SM22!@bigdatadb.polito.it:27017/carsharing?ssl=true&authSource=carsharing&tlsAllowInvalidCertificates=true")

# Get the database
db = client['carsharing']

#ّ Function to derive CDF
def plot_cdf(data_booking, data_parking, label1="Booking", label2="Parking", 
             xlabel="X-axis Label", ylabel="CDF", title="Cumulative Distribution Function"):
    
    sorted_data_booking = np.sort(data_booking)
    yvals_data_booking = np.arange(len(sorted_data_booking)) / float(len(sorted_data_booking))

    sorted_data_parking = np.sort(data_parking)
    yvals_data_parking = np.arange(len(sorted_data_parking)) / float(len(sorted_data_parking))


    plt.plot(sorted_data_booking, yvals_data_booking, marker='.', linestyle='-', color='b', label=label1)
    plt.plot(sorted_data_parking, yvals_data_parking, marker='.', linestyle='-', color='r', label=label2)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.legend()
    plt.grid(True)
    plt.show()

start_date = datetime(2017, 11, 1)
end_date = datetime(2018, 1, 31)

# Calculate booking/parking durations of Torino
pipeline_torino = [
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
            "_id": 0,
            "duration": {
                "$subtract": ["$final_time", "$init_time"]
            }
        }
    }
] 

pipeline_stuttgart = [
    {
        "$match": {
            "city": "Stuttgart",
            "init_date": {
                "$gte": start_date,
                "$lte": end_date
            }
        }
    },
    {
        "$project": {
            "_id": 0,
            "duration": {
                "$subtract": ["$final_time", "$init_time"]
            }
        }
    }
] 

pipeline_columbus = [
    {
        "$match": {
            "city": "Columbus",
            "init_date": {
                "$gte": start_date,
                "$lte": end_date
            }
        }
    },
    {
        "$project": {
            "_id": 0,
            "duration": {
                "$subtract": ["$final_time", "$init_time"]
            }
        }
    }
] 

documents_torino_booking = db["enjoy_PermanentBookings"].aggregate(pipeline_torino)

documents_torino_parking = db["enjoy_PermanentParkings"].aggregate(pipeline_torino)

documents_stuttgart_booking = db["PermanentBookings"].aggregate(pipeline_stuttgart)

documents_stuttgart_parking = db["PermanentParkings"].aggregate(pipeline_stuttgart)

documents_columbus_booking = db["PermanentBookings"].aggregate(pipeline_columbus)

documents_columbus_parking = db["PermanentParkings"].aggregate(pipeline_columbus)


# average, median, standard deviation of Booking in Torino
document_torino_list_booking = np.array([doc['duration'] for doc in documents_torino_booking], dtype='int')
torino_booking_removed_outliers = document_torino_list_booking[(document_torino_list_booking >= 0) & (document_torino_list_booking <= 8000)]

print("Mean, Std and median of Torino Bookings\n")
mean_booking_torino = np.mean(torino_booking_removed_outliers)
std_booking_torino = np.std(torino_booking_removed_outliers)
median_booking_torino = np.median(torino_booking_removed_outliers)

print("Mean: ", mean_booking_torino)
print("Std: ", std_booking_torino)
print("median: ", median_booking_torino, "\n")


# average, median, standard deviation of Parking in Torino
document_torino_list_parking = np.array([doc['duration'] for doc in documents_torino_parking], dtype='int')
torino_parking_removed_outliers = document_torino_list_parking[(document_torino_list_parking >= 0) & (document_torino_list_parking <= 8000)]

print("Mean, Std and median of Torino Parkings\n")
mean_parking_torino = np.mean(torino_parking_removed_outliers)
std_parking_torino = np.std(torino_parking_removed_outliers)
median_parking_torino = np.median(torino_parking_removed_outliers)
print("Mean: ", mean_parking_torino)
print("Std: ", std_parking_torino)
print("median: ", median_parking_torino, "\n")

# average, median, standard deviation of Booking in stuttgart
document_stuttgart_list_booking = np.array([doc['duration'] for doc in documents_stuttgart_booking], dtype='int')
stuttgart_booking_removed_outliers = document_stuttgart_list_booking[(document_stuttgart_list_booking >= 0) & (document_stuttgart_list_booking <= 8000)]

print("Mean, Std and median of stuttgart Booking\n")
mean_booking_stuttgart = np.mean(stuttgart_booking_removed_outliers)
std_booking_stuttgart = np.std(stuttgart_booking_removed_outliers)
median_booking_stuttgart = np.median(stuttgart_booking_removed_outliers)
print("Mean: ", mean_booking_stuttgart)
print("Std: ", std_booking_stuttgart)
print("median: ", median_booking_stuttgart, "\n")

# average, median, standard deviation of Parking in stuttgart
document_stuttgart_list_parking = np.array([doc['duration'] for doc in documents_stuttgart_parking], dtype='int')
stuttgart_parking_removed_outliers = document_stuttgart_list_parking[(document_stuttgart_list_parking >= 0) & (document_stuttgart_list_parking <= 8000)]

print("Mean, Std and median of stuttgart Parking\n")
mean_parking_stuttgart = np.mean(stuttgart_parking_removed_outliers)
std_parking_stuttgart = np.std(stuttgart_parking_removed_outliers)
median_parking_stuttgart = np.median(stuttgart_parking_removed_outliers)
print("Mean: ", mean_parking_stuttgart)
print("Std: ", std_parking_stuttgart)
print("median: ", median_parking_stuttgart, "\n")

# average, median, standard deviation of Booking in columbus
document_columbus_list_booking = np.array([doc['duration'] for doc in documents_columbus_booking], dtype='int')
columbus_booking_removed_outliers = document_columbus_list_booking[(document_columbus_list_booking >= 0) & (document_columbus_list_booking <= 8000)]

print("Mean, Std and median of columbus Booking\n")
mean_booking_columbus = np.mean(columbus_booking_removed_outliers)
std_booking_columbus = np.std(columbus_booking_removed_outliers)
median_booking_columbus = np.median(columbus_booking_removed_outliers)
print("Mean: ", mean_booking_columbus)
print("Std: ", std_booking_columbus)
print("median: ", median_booking_columbus, "\n")

# average, median, standard deviation of Parking in columbus
document_columbus_list_parking = np.array([doc['duration'] for doc in documents_columbus_parking], dtype='int')
columbus_parking_removed_outliers = document_columbus_list_parking[(document_columbus_list_parking >= 0) & (document_columbus_list_parking <= 8000)]

print("Mean, Std and median of columbus Parkings\n")
mean_parking_columbus = np.mean(columbus_parking_removed_outliers)
std_parking_columbus = np.std(columbus_parking_removed_outliers)
median_parking_columbus = np.median(columbus_parking_removed_outliers)
print("Mean: ", mean_parking_columbus)
print("Std: ", std_parking_columbus)
print("median: ", median_parking_columbus, "\n")

# # Percentiles of 25, 50, 75 in Torino
# start_date = datetime(2017, 11, 1)
# end_date = datetime(2018, 1, 31)

# pipeline = [
#     {
#         "$match": {
#             "city": "Torino",
#             "init_date": {
#                 "$gte": start_date,
#                 "$lte": end_date
#             }
#         }
#     },
#     {
#         "$addFields": {
#             "duration": {
#                 "$subtract": ["$final_time", "$init_time"]
#             }
#         }
#     },
#     {
#         "$match": {
#             "duration": {
#                 "$gte": 0,
#                 "$lte": 8000
#             }
#         }
#     },
#     {
#         "$group": {
#             "_id": {
#                 "year": { "$year": "$init_date" },
#                 "month": { "$month": "$init_date" },
#                 "day": { "$dayOfMonth": "$init_date" }
#             },
#             "durations": { "$push": "$duration" }
#         }
#     },
#     {
#         "$sort": {
#             "_id": 1
#         }
#     }
# ]

# # Execute the aggregation pipeline
# documents_torino_booking_pipelines = db["enjoy_PermanentBookings"].aggregate(pipeline)
# documents_torino_parking_pipelines = db["enjoy_PermanentParkings"].aggregate(pipeline)
# documents_stuttgart_booking_pipelines = db["enjoy_PermanentBookings"].aggregate(pipeline)
# documents_stuttgart_parking_pipelines = db["enjoy_PermanentParkings"].aggregate(pipeline)
# documents_columbus_booking_pipelines = db["enjoy_PermanentBookings"].aggregate(pipeline)
# documents_columbus_parking_pipelines = db["enjoy_PermanentParkings"].aggregate(pipeline)


# # Create an empty DataFrame to store the data
# df_booking_torino = pd.DataFrame(columns=['date', '25', '50', '75'])
# df_parking_torino = pd.DataFrame(columns=['date', '25', '50', '75'])
# df_booking_stuttgart = pd.DataFrame(columns=['date', '25', '50', '75'])
# df_parking_stuttgart = pd.DataFrame(columns=['date', '25', '50', '75'])
# df_booking_columbus = pd.DataFrame(columns=['date', '25', '50', '75'])
# df_parking_columbus = pd.DataFrame(columns=['date', '25', '50', '75'])


# # Compute percentiles for each day
# for doc in documents_torino_booking_pipelines:
#     date = datetime(doc['_id']['year'], doc['_id']['month'], doc['_id']['day']).date()
    
#     # To avoid ValueError, check if durations are available for the day
#     if doc['durations']:
#         percentiles = np.percentile(doc['durations'], [25, 50, 75])
#         df_dictionary = pd.DataFrame([{'date': date, '25': percentiles[0], '50': percentiles[1], '75': percentiles[2]}])
#         df_booking_torino = pd.concat([df_booking_torino, df_dictionary], ignore_index=True)

# for doc in documents_torino_parking_pipelines:
#     date = datetime(doc['_id']['year'], doc['_id']['month'], doc['_id']['day']).date()
    
#     # To avoid ValueError, check if durations are available for the day
#     if doc['durations']:
#         percentiles = np.percentile(doc['durations'], [25, 50, 75])
#         df_dictionary = pd.DataFrame([{'date': date, '25': percentiles[0], '50': percentiles[1], '75': percentiles[2]}])
#         df_parking_torino = pd.concat([df_parking_torino, df_dictionary], ignore_index=True)

# for doc in documents_stuttgart_booking_pipelines:
#     date = datetime(doc['_id']['year'], doc['_id']['month'], doc['_id']['day']).date()
    
#     # To avoid ValueError, check if durations are available for the day
#     if doc['durations']:
#         percentiles = np.percentile(doc['durations'], [25, 50, 75])
#         df_dictionary = pd.DataFrame([{'date': date, '25': percentiles[0], '50': percentiles[1], '75': percentiles[2]}])
#         df_booking_stuttgart = pd.concat([df_parking_torino, df_dictionary], ignore_index=True)

# for doc in documents_stuttgart_parking_pipelines:
#     date = datetime(doc['_id']['year'], doc['_id']['month'], doc['_id']['day']).date()
    
#     # To avoid ValueError, check if durations are available for the day
#     if doc['durations']:
#         percentiles = np.percentile(doc['durations'], [25, 50, 75])
#         df_dictionary = pd.DataFrame([{'date': date, '25': percentiles[0], '50': percentiles[1], '75': percentiles[2]}])
#         df_parking_stuttgart = pd.concat([df_parking_torino, df_dictionary], ignore_index=True)

# for doc in documents_columbus_booking_pipelines:
#     date = datetime(doc['_id']['year'], doc['_id']['month'], doc['_id']['day']).date()
    
#     # To avoid ValueError, check if durations are available for the day
#     if doc['durations']:
#         percentiles = np.percentile(doc['durations'], [25, 50, 75])
#         df_dictionary = pd.DataFrame([{'date': date, '25': percentiles[0], '50': percentiles[1], '75': percentiles[2]}])
#         df_booking_columbus = pd.concat([df_parking_torino, df_dictionary], ignore_index=True)

# for doc in documents_columbus_parking_pipelines:
#     date = datetime(doc['_id']['year'], doc['_id']['month'], doc['_id']['day']).date()
    
#     # To avoid ValueError, check if durations are available for the day
#     if doc['durations']:
#         percentiles = np.percentile(doc['durations'], [25, 50, 75])
#         df_dictionary = pd.DataFrame([{'date': date, '25': percentiles[0], '50': percentiles[1], '75': percentiles[2]}])
#         df_parking_columbus = pd.concat([df_parking_torino, df_dictionary], ignore_index=True)


# # Create a new figure and set its size
# plt.figure(figsize=(10, 6))

# # Percentile Plot of Booking Torino
# plt.plot(df_booking_torino['date'], df_booking_torino['25'], label='25th percentile Booking Torino')
# plt.plot(df_booking_torino['date'], df_booking_torino['50'], label='50th percentile Booking Torino')
# plt.plot(df_booking_torino['date'], df_booking_torino['75'], label='75th percentile Booking Torino')


# # Set the title and labels
# plt.title('Percentiles of Booking Durations Over Time Torino')
# plt.xlabel('Date')
# plt.ylabel('Duration')

# # Add a legend
# plt.legend()

# # Display the plot
# plt.show()


# # Percentile Plot of Parking Torino
# plt.plot(df_parking_torino['date'], df_parking_torino['25'], label='25th percentile Parking Torino')
# plt.plot(df_parking_torino['date'], df_parking_torino['50'], label='50th percentile Parking Torino')
# plt.plot(df_parking_torino['date'], df_parking_torino['75'], label='75th percentile Parking Torino')

# # Set the title and labels
# plt.title('Percentiles of Parking Durations Over Time Torino')
# plt.xlabel('Date')
# plt.ylabel('Duration')

# # Add a legend
# plt.legend()

# # Display the plot
# plt.show()


# # Percentile Plot of Booking Stuttgart
# plt.plot(df_booking_stuttgart['date'], df_booking_stuttgart['25'], label='25th percentile Booking Stuttgart')
# plt.plot(df_booking_stuttgart['date'], df_booking_stuttgart['50'], label='50th percentile Booking Stuttgart')
# plt.plot(df_booking_stuttgart['date'], df_booking_stuttgart['75'], label='75th percentile Booking Stuttgart')


# # Set the title and labels
# plt.title('Percentiles of Booking Durations Over Time Stuttgart')
# plt.xlabel('Date')
# plt.ylabel('Duration')

# # Add a legend
# plt.legend()

# # Display the plot
# plt.show()


# # Percentile Plot of Parking Stuttgart
# plt.plot(df_parking_stuttgart['date'], df_parking_stuttgart['25'], label='25th percentile Parking Stuttgart')
# plt.plot(df_parking_stuttgart['date'], df_parking_stuttgart['50'], label='50th percentile Parking Stuttgart')
# plt.plot(df_parking_stuttgart['date'], df_parking_stuttgart['75'], label='75th percentile Parking Stuttgart')

# # Set the title and labels
# plt.title('Percentiles of Parking Durations Over Time Stuttgart')
# plt.xlabel('Date')
# plt.ylabel('Duration')

# # Add a legend
# plt.legend()

# # Display the plot
# plt.show()



# # Percentile Plot of Booking Columbus
# plt.plot(df_booking_columbus['date'], df_booking_columbus['25'], label='25th percentile Booking Columbus')
# plt.plot(df_booking_columbus['date'], df_booking_columbus['50'], label='50th percentile Booking Columbus')
# plt.plot(df_booking_columbus['date'], df_booking_columbus['75'], label='75th percentile Booking Columbus')


# # Set the title and labels
# plt.title('Percentiles of Booking Durations Over Time Columbus')
# plt.xlabel('Date')
# plt.ylabel('Duration')

# # Add a legend
# plt.legend()

# # Display the plot
# plt.show()


# # Percentile Plot of Parking Columbus
# plt.plot(df_parking_columbus['date'], df_parking_columbus['25'], label='25th percentile Parking Columbus')
# plt.plot(df_parking_columbus['date'], df_parking_columbus['50'], label='50th percentile Parking Columbus')
# plt.plot(df_parking_columbus['date'], df_parking_columbus['75'], label='75th percentile Parking Columbus')

# # Set the title and labels
# plt.title('Percentiles of Parking Durations Over Time Columbus')
# plt.xlabel('Date')
# plt.ylabel('Duration')

# # Add a legend
# plt.legend()

# # Display the plot
# plt.show()
