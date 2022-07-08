from kafka import KafkaConsumer
from json import loads
import boto3 
import json
# create our consumer to retrieve the message from the topics
data_stream_consumer = KafkaConsumer(
            bootstrap_servers="localhost:9092",    
            value_deserializer=lambda message: loads(message),
            auto_offset_reset="earliest")

data_stream_consumer.subscribe(topics=["Retaildata"])
s3_client = boto3.client('s3', region_name='us-east-1', aws_access_key_id='************',
                                       aws_secret_access_key='*****************')
for message in data_stream_consumer:
    with open('./data.json', 'w') as f:
        json.dump(message, f)
    response = s3_client.upload_file('data.json', 'pinterest-aicore', 'data.json')


