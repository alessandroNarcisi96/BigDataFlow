# BigDataFlow

## Goal
The goal of this project is to build a data processing pipeline in Python based on Pinterests experiment processing pipeline.

## How does it works?

### Fake the user post
Basically there is a script that simulate a new post from a user.
The script is visible in user_posting_emulation.py.
In RDS the posts are recorded and randomically they are picked and used to be sent

### Handle the post and create a Kafka Producer
I built an Api using FastAPI that will handle the get request.
The code is inside project_pin_API.py and once there I create a Kafka Producer to store it as an event

## Batch

### Handle data with a batch
In batch_consumer.py I create a kafka consumer which gets the data and send them directly to a s3 bucket

### Download the data from s3 and store them in Cassandra
Using boto3 I will download the data and I will store it in Cassandra.
Being a NoSql it has a flexible schema so it works pretty well with changeable data

### Schedule it with Airflow
In data_ingestion.py I built a AirFlow DAG to schedule a python script to get the data from s3,clean them and to send them to Cassandra

## Streaming Data

### Kafka Streaming and PySpark
To manage the same data in streaming I used Spark.
What is Spark? It is a distribuited data processing engine that is very usefull when we have to process a lot of data quickly.
The code to consume the data from Kafka and to process them in Spark it is visible in streaming_consumer.py
