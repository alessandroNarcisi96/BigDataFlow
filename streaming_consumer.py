import findspark
import os
from pyspark.sql import SparkSession
findspark.init()
import multiprocessing
from pyspark.sql.functions import from_json
from pyspark.sql import Row
import pyspark
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 pyspark-shell'
kafka_topic_name = "Retaildata"
# Specify your Kafka server to read data from.
kafka_bootstrap_servers = 'localhost:9092'
# We should always start with session in order to obtain
# context and session if nee
spark= pyspark.sql.SparkSession.builder.config(
            conf=pyspark.SparkConf()
                .setMaster(f"local[{multiprocessing.cpu_count()}]")
                .setAppName("TestApp")).getOrCreate()
stream_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", kafka_topic_name) \
            .option("startingOffsets", "earliest") \
            .load()

#stream_df = stream_df.selectExpr("CAST(value as STRING)")

from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql.functions import col
jsonschema = StructType([ StructField("category",StringType()),StructField("index",StringType()), 
             StructField("unique_id",StringType()), 
             StructField("title", StringType())])

mta_stream =stream_df.select(from_json(col("value").cast("string"), jsonschema).alias("parsed_mta_values"))
mta_data = mta_stream.select("parsed_mta_values.*")
mta_data.writeStream.format("console").outputMode("append").start().awaitTermination()


