from datetime import datetime
import pandas as pd
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from shapely.geometry import Point
from pymongo import MongoClient

# Set up MongoDB connection
client = MongoClient("mongodb://ictts:Ict4SM22!@bigdatadb.polito.it:27017/carsharing?ssl=true&authSource=carsharing&tlsAllowInvalidCertificates=true")

# Get the database
db = client['carsharing']


# # Extract 'origin_destination' field into a separate DataFrame
# origin_destination_df = data['origin_destination'].apply(pd.Series)

# # Extract the coordinates
# coordinates = origin_destination_df['coordinates'].tolist()

# Random Longitude and Latitude selected from DB
random_longitude = 7.69458
random_latitude = 45.1009

""" At the latitude of Torino, Italy, one degree of longitude is approximately 71703 meters and
    one degree of latitude is approximately 111045 meters. Assuming that these measurements are
    accurate enough for your purposes, a square of 250,000 m² would have sides of approximately √250000 m = 500 m. 
    To convert these distances into degrees, we divide them by the length of one degree in meters.
    500 m ≈ 500/71703 degrees of longitude = 0.00697 degrees of longitude
    500 m ≈ 500/111045 degrees of latitude = 0.00450 degrees of latitude. """

# Calculate the change in longitude and latitude for a 500 m distance
delta_longitude = 500 / 71703
delta_latitude = 500 / 111045

# Choose four points that form a square
polygon_coordinates = [
    [random_longitude - delta_longitude/2, random_latitude - delta_latitude/2],  # Bottom-left corner
    [random_longitude - delta_longitude/2, random_latitude + delta_latitude/2],  # Top-left corner
    [random_longitude + delta_longitude/2, random_latitude + delta_latitude/2],  # Top-right corner
    [random_longitude + delta_longitude/2, random_latitude - delta_latitude/2],  # Bottom-right corner
    [random_longitude - delta_longitude/2, random_latitude - delta_latitude/2]   # Back to the first point to close the polygon
]

start_date = datetime(2017, 11, 1)
end_date = datetime(2018, 1, 31)

pipeline_torino = {
            "city": "Torino",
            "init_date": {
                "$gte": start_date,
                "$lte": end_date
            }, 
        'origin_destination.coordinates.1': {
            '$geoWithin': {
                '$geometry': {
                    'type': 'Polygon',
                    'coordinates': [polygon_coordinates]}}}}

# Load the data into a pandas DataFrame
results = db["PermanentBookings"].find(pipeline_torino)


# Create an empty list to store coordinates
coords_list = []

# Loop through results and add each to the coordinates list
for result in results:
    # The coordinates are in the form [longitude, latitude]
    coords = result['origin_destination']['coordinates'][1]
    coords_list.append(coords)

# Convert the list of coordinates to a GeoDataFrame
gdf = gpd.GeoDataFrame(geometry=gpd.points_from_xy([coords[0] for coords in coords_list], 
                                                   [coords[1] for coords in coords_list]))

# Create a 2D histogram of car locations
heatmap_data, xedges, yedges = np.histogram2d(gdf.geometry.x, gdf.geometry.y, bins=(10, 10))

# Create a heatmap plot
plt.figure(figsize=(10, 10))
sns.heatmap(heatmap_data, cmap='YlOrRd')

# Show the plot
plt.show()