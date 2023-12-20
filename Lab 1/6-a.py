import webbrowser
import folium
from folium.plugins import HeatMap
from pymongo import MongoClient
from datetime import datetime

# Set up MongoDB connection
client = MongoClient("mongodb://ictts:Ict4SM22!@bigdatadb.polito.it:27017/carsharing?ssl=true&authSource=carsharing&tlsAllowInvalidCertificates=true")

# Get the database
db = client['carsharing']

start_date = datetime(2017, 11, 1)
end_date = datetime(2018, 1, 31)

# Define aggregation pipeline
pipeline = [
    {
        "$addFields": {
            "day_of_week": { "$dayOfWeek": "$final_date" },
            "hour": { "$hour": "$final_date" },
            "duration": {
                "$subtract": ["$final_time", "$init_time"]}
        }
    },
    {
        "$match": {
            "day_of_week": 2,  # MongoDB uses 1-indexed days of the week, so Monday is 2
            "hour": { "$gte": 6, "$lte": 8 },
            "duration": { "$gte": 0, "$lte": 8000 }
        }
    },
    {
        "$group": {
            "_id": { "$arrayElemAt": [ "$origin_destination.coordinates", 1 ] },  # Group by destination coordinates at index 1
            "count": { "$sum": 1 }  # Sum the number of cars for each group
        }
    },
]


# Execute aggregation
agg_result = list(db["PermanentBookings"].aggregate(pipeline))

heatmap_data = [[result["_id"][1], result["_id"][0], result["count"]] for result in agg_result]

# Create a Map instance
m = folium.Map(location=[heatmap_data[0][0], heatmap_data[0][1]], zoom_start=13)

# Add HeatMap to the map instance
HeatMap(heatmap_data).add_to(m)

# Save the map to an HTML file
m.save('heatmap.html')
webbrowser.open('heatmap.html')