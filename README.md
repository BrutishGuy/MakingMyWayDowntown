
# Introduction

This project is a Hadoop experimentation project looking to perform various data analyses on taxi trips in San Francisco via GPS tracking data. 

# Build and Install

This installation/build procedure is optimized for Linux. Unfortunately installation of Hadoop on Windows is a non-trivial exercise. Follow these steps:

* Ensure you have Hadoop installed. Make sure to set the correct environment variables, as shown in the subsequent steps:
* Set your working environment to the src folder
* In a terminal, run the command `mvn clean package`
* You are done. You should see an Exercise2.jar executable in the home directory of this repository.

# Running Examples

## Trip Length Distribution

To run the code associated with Exercise 1, specifically the Trip Length Distribution, please run 

python3 exercise1.py 

The environment variables there have already been set to defaults for the virtual machines on the cluster, but optional parameters are specified.
This Spark aspect was done in Python, including using PySpark, as there was no restriction on this either in the assignment.

$SPARK_INSTALL/bin/spark-submit --class Assignment3.LengthDistribution --master local[*] Excercise1_spark/target/Excercise1.jar <PATH_TO_INPUT_FILE> <PATH_TO_OUTPUT_FILE> <NUMBER_OF_BINS> <FIND_OUTLIERS?:true|false>

## Trip Revenue Distribution

Run the below command (example usage shown below as well) to calculate the total revenue as well as output files for the trip revenue distribution

hadoop jar Excercise2/target/Excercise2.jar Assignment3.AirportRideRevenueMain <PATH_TO_INPUT_FILE> <PATH_TO_OUTPUT_FILE> <NO_OF_REDUCERS_STAGE_1> <NO_OF_REDUCERS_STAGE_2> <CONSIDER_OVERLAPPING_SEGMENTS?:true|false> <RECONSTRUCT_AIRPORT_TRIPS_ONLY?:true|false>

Example usage:
hadoop jar Excercise2/target/Excercise2.jar Assignment3.AirportRideRevenueMain /data/all.segments /user/r0605648/output 9 1 true true

