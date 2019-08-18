import sys
sys.path.append("./utility/")

import time
import json
import boto3
import lazyreader
import utility
from kafka.producer import KafkaProducer

class MyKafkaProducer(object):
    """
    class that implements Kafka producers that ingest data from S3 bucket
    """

    def __init__(self, kafka_configfile, schema_file, s3_configfile):
        """
        class constructor that initializes the instance according to the configurations
        of the S3 bucket and Kafka
        :type kafka_configfile: str     path to kafka config file
        :type schema_file     : str     path to schema file
        :type s3_configfile   : str     path to S3 config file
        """
        self.kafka_config = utility.parse_config(kafka_configfile)

        self.schema = utility.parse_config(schema_file)
        self.s3_config = utility.parse_config(s3_configfile)

        self.producer = KafkaProducer(bootstrap_servers=self.kafka_config["BROKERS_IP"])


    def get_key(self, msg):
        """
        produces key for message to Kafka topic
        :type msg: dict     message for which to generate the key
        :rtype   : str      key that has to be of type bytes
        """
        msgwithkey = utility.add_block_fields(msg)
        if msgwithkey is None:
            return
        x, y = msgwithkey["block_lonid"], msgwithkey["block_latid"]
        return str((x*137+y)%77703).encode()


    def produce_msgs(self):
        """
        produces messages and sends them to topic
        """
        msg_cnt = 0

        while True:

            s3 = boto3.client('s3')
            # obj = s3.get_object(Bucket=self.s3_config["BUCKET"],
            #                     Key="{}/{}".format(self.s3_config["FOLDER"],
            #                                        self.s3_config["STREAMING_FILE"]))

            obj = s3.get_object(Bucket='nyctaxitrip',
                    Key="{}/{}".format('yellow_trip',
                                       'yellow_tripdata_sample.csv'))

            for line in lazyreader.lazyread(obj['Body'], delimiter='\n'):

                message_info = line.strip()
                msg = utility.map_schema(message_info, self.schema)

                if msg is not None:
                    self.producer.send(self.kafka_config["TOPIC"],
                                       value=json.dumps(msg),
                                       key=self.get_key(msg))
                    msg_cnt += 1

                time.sleep(0.001)