import sys
sys.path.append("./utility/")

from producers import MyKafkaProducer


### main method that produces messages into Kafka topic ###

if __name__ == "__main__":

    if len(sys.argv) != 4:
        sys.stderr("Usage: main_produce.py <kafkaconfigfile> <schemafile> <s3configfile> \n")
        sys.exit(-1)

    kafka_configfile, schema_file, s3_configfile = sys.argv[1:4]

    prod = MyKafkaProducer(kafka_configfile, schema_file, s3_configfile)
    prod.produce_msgs()