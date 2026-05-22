from delta import *
from delta.tables import *
from delta.tables import DeltaTable
import csv
from confluent_kafka import Producer,Consumer
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext
import time
import pyspark
import json
import os
import urllib.parse
from pyspark.sql.functions import *
import json
import sys
import traceback
from pyspark.sql.types import *
from sparkmeasure import StageMetrics
from pyspark.sql.functions import from_unixtime, unix_timestamp
import psutil
import time


bootstrap_servers = 'localhost:9092'
topic_name = 'csv_topic'
builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("INFO")

csv_file_path = 'E:/MobiAct\MobiAct_Dataset_v2.0/Annotated Data/BSC/BSC_1_1_annotated.csv'

# Kafka Producer configuration
producer_config = {
    'bootstrap.servers': "localhost:9092",
}
def get_kafka_cpu_usage():
    # Get the percentage of CPU usage for the Kafka process
    kafka_process_name = "java"  # Adjust this based on your Kafka process name
    for process in psutil.process_iter(['pid', 'name', 'cpu_percent']):
        if kafka_process_name in process.info['name']:
            return process.info['cpu_percent']
    return None
def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# Create a Kafka producer
producer = Producer(producer_config)
record_count=5

with open(csv_file_path, mode='r') as csvfile:
     reader = csv.reader(csvfile)
     header = next(reader)  # Assuming the first row is the header
     data = [row for row in reader][:record_count]
 # Write selected records to the output CSV file
with open("records.csv", mode='w', newline='') as output_file:
     writer = csv.writer(output_file)
     writer.writerow(header)  # Write the header to the output file
     writer.writerows(data)
# Start a timer to measure the processing time
start_time = time.time()
try:
    with open("records.csv", 'r') as csvfile:
        csvreader = csv.DictReader(csvfile)
        for row in csvreader:
            # Produce each CSV row as a message to the Kafka topic
            message_value = ','.join([f'{key}:{value}' for key, value in row.items()])
            producer.produce(topic=topic_name, value=message_value, callback=delivery_report)
            producer.flush()
            print("CSV data successfully ingested into Kafka topic.")
            kafka_cpu_usage = get_kafka_cpu_usage()
            if kafka_cpu_usage is not None:
                    print(f'Kafka CPU Utilization: {kafka_cpu_usage}%')
            else:
                    print('Kafka process not found.')
            time.sleep(1)

    

except Exception as e:
    print(f"An error occurred: {str(e)}")
finally:
    producer.flush()
    
    
c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})


def convert_to_json(msg):
    split_values = msg.split(',')
    
    # Create a dictionary from the split values
    data_dict = {}
    for item in split_values:
        key, value = item.split(':')
        data_dict[key] = float(value) if '.' in value else int(value) if value.isdigit() else value
    
    # Convert the dictionary to a JSON string
    json_data = json.dumps(data_dict)
    #print(json_data)
    with open("kafka.json", "a") as outfile:
        outfile.write(json_data)
    return json_data

c.subscribe([topic_name] )
counter = 0
t=0
#Read the first few rows to infer the schema
#infer_schema_df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_file_path )

# Extract the inferred schema
#infer_schema = infer_schema_df.schema


try:
    while True:
        if t==record_count:
            break
        msg = c.poll(timeout=1)
        #print(counter)
        if counter == 5:
            break
        if msg is None:
            print('No message received')
            counter += 1
            continue

        if msg.error():
            print(f'Error: {msg.error()}')
            continue

        print(f'Received message: {msg.value().decode("utf-8")}')
        # Split the string by commas
        msg=f'{msg.value().decode("utf-8")}'
        convert_to_json(msg)
        t+=1
        # Increase the counter every time poll() fetches 0 records from Kafka Brokers
        # Reset the counter to 0 if poll() extracts records
        # If the counter reaches a certain threshold (say 10), break out of the loop and close the consumer
        
    # Convert the JSON string to a Spark DataFrame
    df = spark.read.format("json").load("kafka.json")

     #df.show()
     # Write the DataFrame to Delta Lake
    df.write.format('delta').mode("append").save("Gad/BSC")
    print("Done")
    end_time = time.time()
    # Calculate the processing time
    processing_time = end_time - start_time

    # Calculate the data ingestion rate
    data_ingestion_rate = record_count / processing_time

    # Print the results
    print(f"Number of Records Ingested: {record_count}")
    print(f"Processing Time: {processing_time} seconds")
    print(f"Data Ingestion Rate: {data_ingestion_rate} records per second")

    spark.stop()
    
except KeyboardInterrupt:
    print('Interrupted through kb')
finally:
    c.close()

