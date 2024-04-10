from delta import *
from delta.tables import *
from delta.tables import DeltaTable
import csv
import pyspark
from confluent_kafka import Producer,Consumer
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext
import argparse
import logging
import sys
import pandas as pd
from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy
from pyspark.sql import SparkSession
from pyflink.common import Time, WatermarkStrategy, Duration
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig
from pyflink.datastream.formats import csv,json
from pyflink.table import DataTypes
import json
import csv
import os
import time

df = pd.read_csv('E:/MobiAct\MobiAct_Dataset_v2.0/Annotated Data/BSC/BSC_1_1_annotated.csv')



def emit_one_record_per_second(element):
    c=0
    print("CSV record {c} successfully ingested.")
    time.sleep(1)# Sleep for 1 second to simulate real-time processing
    c+=1
    return element    
    
def Ingest_MobiAct(input_path, output_path,num_records):
   
    csv_file_path='E:/MobiAct\MobiAct_Dataset_v2.0/Annotated Data/BSC/BSC_1_1_annotated.csv'
    #json_file_path='E:/MobiAct/MobiAct_Dataset_v2.0/Annotated Data/BSC/data.json'
    #source = FileSource.for_record_stream_format(stream_format=json(),json_file_path).build()
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    
    # write all the data to one file
    env.set_parallelism(1)
    # Read data from the input CSV file
    c=int(num_records)
    with open(csv_file_path, mode='r') as csvfile:
         reader = csv.reader(csvfile)
         header = next(reader)  # Assuming the first row is the header
         data = [row for row in reader][:c]
     # Write selected records to the output CSV file
    with open("records.csv", mode='w', newline='') as output_file:
         writer = csv.writer(output_file)
         writer.writerow(header)  # Write the header to the output file
         writer.writerows(data)
    # define the source
    if input_path is not None:
      
        ds = env.from_source(
            source=FileSource.for_record_stream_format(StreamFormat.text_line_format(),
                                                       input_path)
                           
                             .process_static_file_set().build(),
            watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
            source_name='csv-source'
        )
    else:
        print("Executing MobiAct with default input data set.")
        print("Use --input to specify file input.")
       
            
        source = FileSource \
            .for_record_stream_format(StreamFormat.text_line_format(), "records.csv") \
            .build()#.monitor_continuously(Duration.
        ds = env.from_source(source, WatermarkStrategy.no_watermarks(), 'csv-source')
        # Define a function to simulate ingesting one record per second

    # Apply the function to the stream to simulate one record per second
        result_stream = ds.map(emit_one_record_per_second)
        # Print the result stream
        result_stream.print()

    def split(line):
        yield from line.split()
    

    # define the sink
    if output_path is not None:
                  
        ds.sink_to(
            sink=FileSink.for_row_format(
                base_path=output_path, encoder=Encoder.simple_string_encoder())
            .with_output_file_config( OutputFileConfig.builder()
        .with_part_prefix("prefix")
        .with_part_suffix(".csv")
        .build())
    .build()
        )
       
        
    else:
        print("Printing result to stdout. Use --output to specify output path.")
        ds.print()
    # submit for execution
    env.execute()

def list_of_files(root_path):
        files = []
        names=[]
        dirpath=None
        for dirpath, dirnames, filenames in os.walk(root_path):
            print(f"{dirpath}")
            dirpath=dirpath
            for filename in filenames:
                # Split the filename and extension
                if filename.endswith((".csv")):
                    files.append(os.path.join(dirpath, filename))
                    name, extension = os.path.splitext(filename)
                    names.append(name)
        return files,names,dirpath
def decode_ext_to_json(csvFilePath):
        jsonArray = []
        file=[]
        file,name,dirpath=list_of_files(csvFilePath)
        #read csv file
        with open(f'{file[0]}', encoding='utf-8') as csvf: 
            #load csv file data using csv library's dictionary reader
            csvReader = csv.DictReader(csvf) 

            #convert each csv row into python dict
            for row in csvReader: 
                #add this python dict to json array
                jsonArray.append(row)
        n=f"{dirpath}/{name[0]}.json"
        #convert python jsonArray to JSON String and write to file
        with open(n, 'w', encoding='utf-8') as jsonf: 
            jsonString = json.dumps(jsonArray)
            jsonf.write(jsonString)
        return  n

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    # Create a Spark session
    
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        help='Output file to write results to.')
    parser.add_argument(
        '--DeltaTable',
        dest='DeltaTable',
        required=False,
        help='Assign delta lakehouse table name')
    parser.add_argument(
        '--Records',
        dest='Records',
        required=False,
        help='Number of records to be ingested')

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)
    # Start a timer to measure the processing time
    start_time = time.time()
    Ingest_MobiAct(known_args.input, known_args.output,known_args.Records)
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    spark.sparkContext.setLogLevel("INFO")
    #Read the first few rows to infer the schema
    #infer_schema_df = spark.read.option("header", "true").option("inferSchema", "true").csv('E:/MobiAct\MobiAct_Dataset_v2.0/Annotated Data/BSC/BSC_1_1_annotated.csv' )

    # Extract the inferred schema
    #infer_schema = infer_schema_df.schema
    name=decode_ext_to_json(f"E:/Lakehouse/{known_args.output}/")
    
    # Convert the JSON string to a Spark DataFrame
    df = spark.read.format("json").load(name)
    #record_count+=df.count()
    df.show()
    # Write the DataFrame to Delta Lake
    df.write.format('delta').mode("append").save(f'{known_args.DeltaTable}/')
    print("Done")
    end_time = time.time()
    # Calculate the processing time
    processing_time = end_time - start_time
    record_count=int(known_args.Records)
    # Calculate the data ingestion rate
    data_ingestion_rate = record_count / processing_time

    # Print the results
    print(f"Number of Records Ingested: {record_count}")
    print(f"Processing Time: {processing_time} seconds")
    print(f"Data Ingestion Rate: {data_ingestion_rate} records per second")
    spark.stop()
