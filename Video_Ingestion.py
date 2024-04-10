# -*- coding: utf-8 -*-
"""
Created on Mon Oct 30 21:45:24 2023

@author: Ahmed Harby
"""

from pyspark.sql import SparkSession
import os
from delta import *
import pyspark
from pyspark.sql.functions import *
from delta.tables import *
import json
import sys
import traceback
from pyspark.sql.types import *
from sparkmeasure import StageMetrics
from pyspark.sql.functions import from_unixtime, unix_timestamp
import time
import pandas as pd
import gc
from pyspark import StorageLevel

# Your code here


builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")\
    .config("spark.executor.memory", "8g")\
    .config("spark.driver.memory", "6g")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# Define the root directory where video files are located
root_directory = "E:/Video_Dataset/dataset/"



def list_of_videos(root_path):
    files = []
    names=[]
    for dirpath, dirnames, filenames in os.walk(root_path):
        print(f"{dirpath}")
        for filename in filenames:
            # Split the filename and extension
            if filename.endswith((".mp4", ".avi", ".mkv")):
                files.append(os.path.join(dirpath, filename))
                name, extension = os.path.splitext(filename)
                # Print only the file name (without extension)
                names.append(name)
    return files,names


# List all video files in the root directory and its subdirectories
video_files,video_names = list_of_videos(root_directory)

# Count the number of items in the list
count_of_videos= len(video_names)
record_count=0
# Stop the Spark session
stagemetrics = StageMetrics(spark)
stagemetrics.begin()

# Start a timer to measure the processing time
start_time = time.time()
for index, element in enumerate(video_files):
    #print(f"Index: {index}, Element: {element}")
    if index==1000:
        break
    df = spark.read.format('binaryFile').load(f"{element}")
    #df.show()
    #df.persist(StorageLevel.OFF_HEAP)
    #df.cache()
    n=video_names[index]
    record_count += df.count()
    df.write.format("delta").save(f"UCL_7/{n}")
    # Explicitly run garbage collector
    df.unpersist()
    gc.collect()

end_time = time.time()
stagemetrics.end()
stagemetrics.print_report()  


# Calculate the processing time
processing_time = end_time - start_time

# Calculate the data ingestion rate
data_ingestion_rate = record_count / processing_time
 
# Print the results
print(f"Number of Records Ingested: {record_count}")
print(f"Processing Time: {processing_time} seconds")
print(f"Data Ingestion Rate: {data_ingestion_rate} records per second")


# Stop the Spark session when done
spark.stop()
