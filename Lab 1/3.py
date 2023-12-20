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



document_torino_list_booking = np.array([doc['duration'] for doc in documents_torino_booking], dtype='int')
torino_booking_removed_outliers = document_torino_list_booking[(document_torino_list_booking >= 0) & (document_torino_list_booking <= 8000)]

document_torino_list_parking = np.array([doc['duration'] for doc in documents_torino_parking], dtype='int')
torino_parking_removed_outliers = document_torino_list_parking[(document_torino_list_parking >= 0) & (document_torino_list_parking <= 8000)]

document_stuttgart_list_booking = np.array([doc['duration'] for doc in documents_stuttgart_booking], dtype='int')
stuttgart_booking_removed_outliers = document_stuttgart_list_booking[(document_stuttgart_list_booking >= 0) & (document_stuttgart_list_booking <= 8000)]

document_stuttgart_list_parking = np.array([doc['duration'] for doc in documents_stuttgart_parking], dtype='int')
stuttgart_parking_removed_outliers = document_stuttgart_list_parking[(document_stuttgart_list_parking >= 0) & (document_stuttgart_list_parking <= 8000)]

document_columbus_list_booking = np.array([doc['duration'] for doc in documents_columbus_booking], dtype='int')
columbus_booking_removed_outliers = document_columbus_list_booking[(document_columbus_list_booking >= 0) & (document_columbus_list_booking <= 8000)]

document_columbus_list_parking = np.array([doc['duration'] for doc in documents_columbus_parking], dtype='int')
columbus_parking_removed_outliers = document_columbus_list_parking[(document_columbus_list_parking >= 0) & (document_columbus_list_parking <= 8000)]

plot_cdf(torino_booking_removed_outliers, torino_parking_removed_outliers, xlabel="Duration", ylabel="CDF", title="Cumulative Distribution Function of Durations At Torino")
plot_cdf(stuttgart_booking_removed_outliers, stuttgart_parking_removed_outliers, xlabel="Duration", ylabel="CDF", title="Cumulative Distribution Function of Durations At Stuttgart")
plot_cdf(columbus_booking_removed_outliers, columbus_parking_removed_outliers, xlabel="Duration", ylabel="CDF", title="Cumulative Distribution Function of Durations At Columbus")



