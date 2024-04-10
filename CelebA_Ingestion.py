# -*- coding: utf-8 -*-
"""
Created on Tue Apr  9 22:17:21 2024

@author: Ahmed Harby
"""
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

def CelebA():
    
    root_directory=""
    
    def list_of_images(root_path):
        files = []
        names=[]
        for dirpath, dirnames, filenames in os.walk(root_path):
            print(f"{dirpath}")
            for filename in filenames:
                # Split the filename and extension
                if filename.endswith((".jpg")):
                    files.append(os.path.join(dirpath, filename))
                    name, extension = os.path.splitext(filename)
                # Print only the file name (without extension)
                    names.append(name)
        return files,names
    def list_of_csv(root_path):
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

    #print(video_files)
    # List all files in the root directory and its subdirectories
    files_images,image_names = list_of_images(root_directory)
    
    files_csv,csv_names = list_of_csv(root_directory)
    # Count the number of items in the list
    count_of_images = len(image_names)
    # Print the result
    print(f"Number of images in the list: {count_of_images}")
    
    count_of_csv = len(csv_names)
    # Print the result
    print(f"Number of csv in the list: {count_of_csv}")
    
    record_count=0
    # Stop the Spark session
    stagemetrics = StageMetrics(spark)
    stagemetrics.begin()
    # Start a timer to measure the processing time
    start_time = time.time()
    for index, element in enumerate(files_images):
        if index==1000: #use mini batch of 1000 images
            break
        #print(f"Index: {index}, Element: {element}")
        df = spark.read.format('image').load(f"{element}")
        #df.show()
        #df.persist(StorageLevel.DISK_ONLY)
        #df.cache()
        n=image_names[index]
        record_count += df.count()
        df.write.format("delta").save(f"celebA_7/{n}")
        # Explicitly run garbage collector
        df.unpersist()
        gc.collect()
    
    for index, element in enumerate(files_csv):
        #print(f"Index: {index}, Element: {element}")
        df = spark.read.format('csv').load(f"{element}")
        #df.show()
        df.persist(StorageLevel.DISK_ONLY)
        #df.cache()
        n=csv_names[index]
        record_count += df.count()
        df.write.format("delta").save(f"celebA_7/{n}")
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
    
    return
