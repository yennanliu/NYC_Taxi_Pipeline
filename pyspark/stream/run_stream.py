import sys
from stream_processor import TaxiStreamer



### main method that executes streaming job ###

if __name__ == '__main__':

    if len(sys.argv) != 5:
        sys.stderr.write("Usage: spark-submit --packages <packages> main_stream.py <kafkaconfigfile> <schemaconfigfile> <streamconfigfile> <postgresconfigfile> \n")
        sys.exit(-1)

    kafka_configfile, schema_configfile, stream_configfile, psql_configfile = sys.argv[1:5]

    streamer = TaxiStreamer(kafka_configfile, schema_configfile, stream_configfile, psql_configfile)
    streamer.run()