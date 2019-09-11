import time
import json
import boto3
import os 
from kafka.producer import KafkaProducer

if __name__ == '__main__':
    AWSAccessKeyId = os.environ['AWSAccessKeyId']
    AWSSecretKey = os.environ['AWSSecretKey']
    s3 = boto3.client('s3',
         aws_access_key_id=AWSAccessKeyId,
         aws_secret_access_key= AWSSecretKey)
    producer = KafkaProducer(bootstrap_servers="127.0.0.1:9092")
    obj = s3.get_object(Bucket='nyctaxitrip',
            Key="{}/{}".format('yellow_trip',
                               'yellow_tripdata_sample.csv'))  # read s3 csv 
    lines = str(obj['Body'].read())
    for line in lines.split("\\n"):
        print (json.dumps(line))
        producer.send("new_topic",
                      value=line.encode(),    # encode the value to enable kafka consumer to recieve the stream msg
                      key=b'key')             # encode the key to enable kafka consumer to recieve the stream msg
        time.sleep(0.1)