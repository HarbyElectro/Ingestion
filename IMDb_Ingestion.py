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
import os
import gc
from pyspark import StorageLevel
import psutil
import csv
#from java.io.charset import StandardCharsets
#from org.apache.commons.io import IOUtils
#from org.apache.nifi.processor.io import StreamCallback
#from org.python.core.util import StringUtil
import warnings

def fxn():
    warnings.warn("deprecated", DeprecationWarning)

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    fxn()

# Or if you are using > Python 3.11:
with warnings.catch_warnings(action="ignore"):
    fxn()


builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")\
    .config("spark.executor.memory", "8g")\
    .config("spark.driver.memory", "6g")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("INFO")

def IMDB():
    root_directory="E:/IMDB_Dataset/imdb_csv_files/sql_csv_files/"
    # Function to list video files recursively
    def list_of_files(root_path):
        files = []
        names=[]
        for dirpath, dirnames, filenames in os.walk(root_path):
            print(f"{dirpath}")
            for filename in filenames:
                # Split the filename and extension
                if filename.endswith((".csv")):
                    files.append(os.path.join(dirpath, filename))
                    name, extension = os.path.splitext(filename)
                # Print only the file name (without extension)
                    names.append(name)
        return files,names


    # List all video files in the root directory and its subdirectories
    files,names = list_of_files(root_directory)
    
    print("Ingestion Started!")
    record_count=0
    # Stop the Spark session
    stagemetrics = StageMetrics(spark)
    stagemetrics.begin()
    # Start a timer to measure the processing time
    start_time = time.time()
    for index, element in enumerate(files):
        #print(f"Index: {index}, Element: {element}")
        df = spark.read.format('csv').load(f"{element}")
        #df.show()
        #df.cache()
        #df.persist(StorageLevel.OFF_HEAP)
        n=names[index]
        record_count += df.count()
        df.write.format("delta").save(f"IMDB_10/{n}")
        # Explicitly run garbage collector
        df.unpersist()
        gc.collect()
    end_time = time.time()

    stagemetrics.end()
    stagemetrics.print_report()  
    # Stop the timer

    # Calculate the processing time
    processing_time = end_time - start_time

    # Calculate the data ingestion rate
    data_ingestion_rate = record_count / processing_time
    print("Ingestion ended!------------------------------------------------")
    # Print the results
    print(f"Number of Records Ingested: {record_count}")
    print(f"Processing Time: {processing_time} seconds")
    print(f"Data Ingestion Rate: {data_ingestion_rate} records per second")
   
 
    #IMDP dataset
    return