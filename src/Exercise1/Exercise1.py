# -*- coding: utf-8 -*-
"""
@author: VictorGueorguiev - r0781168
"""

### Do necessary imports
import matplotlib.pyplot as plt
import os 
import findspark

#spark_location = "C:\\Users\\VictorGueorguiev\\Documents\\Spark\\spark-2.4.5-bin-hadoop2.7"
#os.environ['JAVA_HOME'] = "C:\\Program Files\\Java\\jdk1.8.0_161"
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
import math
from datetime import datetime
import time
import argparse
## returns result in kilometres
## this is more accurate that using the flat earth version defined below
def haversine(lat1, lon1, lat2, lon2):
    R = 6373.0  ## Earth radius in kilometers
    
    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)
    
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    distance = R * c
    return distance

def spherical_projection(lat1, lon1, lat2, lon2):
    R = 6373.0  ## Earth radius in kilometers
    
    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)
    
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    midlat = (lat1 + lat2)/2.0
    
    
    distance = R * math.sqrt(dlat**2 + (math.cos(midlat) * dlon)**2) * 1000
    return distance


# Model Settings =================================================================================================
parser = argparse.ArgumentParser(description='Exercise1',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument('--dataset_name', type=str, default='./data/2010_03.trips',
                    help='Dataset name and full path.')
parser.add_argument('--spark_location', type=str, default="/cw/bdap/software/spark-2.4.0-bin-hadoop2.7", help='Spark install location, default on cw/bdap...')
parser.add_argument('--java_home', type=str, default='/usr/lib/jvm/java-8-openjdk-amd64/', help='JAVA HOME env variable, default is /usr/lib/jvm/java-8-openjdk-amd64/')
parser.add_argument('--do_plotting', type=bool, default=False, help='Should we plot results? Default is False since running on remote machines.')

opt = parser.parse_args()

#spark_location = "/cw/bdap/software/spark-2.4.0-bin-hadoop2.7"
#os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64/'
#data_folder = '/cw/bdap/assignment3/'

spark_location = opt.spark_location
os.environ['JAVA_HOME'] = opt.java_home
dataset_name = opt.dataset_name
do_plotting = opt.do_plotting

### TESTING SPARK CONNECTION
#spark = SparkSession.builder.getOrCreate()
#df = spark.sql("select 'spark' as hello ")
#df.show()
findspark.init(spark_location)

conf = pyspark.SparkConf()
# conf.set('spark.app.name', app_name) # Optional configurations

# init & return
sc = pyspark.SparkContext.getOrCreate(conf=conf)

sqlContext = SQLContext(sc)


### PART A - TRIP DISTANCE DISTRIBUTION BY HAND
                        
## Homemade calculations, including loading data


f = open(dataset_name, "r")

start = time.time()

homemade_trip_distribution = []

for line in f.readlines():
    line = line.strip().split(" ")
    start_time = float(line[1])
    end_time = float(line[4])
    lat1 = float(line[2])
    lon1 = float(line[3])
    lat2 = float(line[5])
    lon2 = float(line[6])
    
    if lat1 <= 39.0 and lat1 >= 36.0 and lon1 >= -124.0 and lon1 <= -121.0 and lat2 <= 39.0 and lat2 >= 36.0 and lon2 >= -124.0 and lon2 <= -121.0:
        if end_time - start_time > 0.0:
            distance_temp = haversine(lat1, lon1, lat2, lon2)
            if 3.6 * distance_temp/(end_time - start_time) < 200.0:            
                homemade_trip_distribution.append(distance_temp)
end = time.time()
elapsed = end - start
print("Time elapsed for the home-made trip distance distribution: ", str(elapsed), "seconds")
f.close()

# plot data in RDD - use .collect() to bring data to local
num_bins = 35
if do_plotting:
    n, bins, patches = plt.hist(homemade_trip_distribution, num_bins, normed=0, facecolor='green', alpha=0.5, range=[0, 35])
    n, bins, patches = plt.hist(homemade_trip_distribution, num_bins, normed=1, facecolor='green', alpha=0.5, range=[0, 35])


### PART B - TRIP DISTANCE DISTRIBUTION USING PYSPARK

## PySpark RDD API calculations, including loading data


text_file = sc.textFile(dataset_name)

start = time.time()

taxi_trip_sparkdf = text_file.map(lambda line: line.split(" "))

taxi_trip_sparkdf = taxi_trip_sparkdf \
                    .map(lambda x: (float(x[2]), float(x[3]), float(x[5]), float(x[6]), float(x[1]), float(x[4])))
## clean outliers and errors
taxi_trip_sparkdf = taxi_trip_sparkdf \
                    .filter(lambda x: x[0] <= 39.0 and x[0] >= 36.0 and x[1] >= -124.0 and x[1] <= -121.0 and x[2] <= 39.0 and x[2] >= 36.0 and x[3] >= -124.0 and x[3] <= -121.0)\
                    .filter(lambda x: x[4] - x[1] > 0.0)\
                    .filter(lambda x: 3.6 * haversine(x[0], x[1], x[2], x[3])/(x[4] - x[1]) < 200.0)

## Having considered outliers and removing them, we calculate a geodesic approximation to distance as
## given by the Haverside function above and extract the maximum distance trip 
## and return the max (taxi_id, trip_distance) key-value pair.
pyspark_trip_distribution = taxi_trip_sparkdf \
                     .map(lambda taxi_trip: haversine(taxi_trip[0], taxi_trip[1], taxi_trip[2], taxi_trip[3])) \
                     .collect()

end = time.time()
elapsed = end - start
print("Time elapsed for PySpark RDD API trip distance distribution: ", str(elapsed), "seconds")
## ready for plotting with matplotlib.pyplot
             

# plot data in RDD - use .collect() to bring data to local
num_bins = 36
if do_plotting:
    n, bins, patches = plt.hist(pyspark_trip_distribution, num_bins, normed=0, facecolor='green', alpha=0.5,  range=[0, 35])
    n, bins, patches = plt.hist(pyspark_trip_distribution, num_bins, normed=1, facecolor='green', alpha=0.5,  range=[0, 35])


        
                     