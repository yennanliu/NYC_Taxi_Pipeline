spark-submit --master spark://$SPARK_STREAM_CLUSTER_0:7077 \
             --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 \
             --jars $PWD/mysql-connector-java-5.1.30-bin.jar \
             --py-files $AUX_FILES \
             --driver-memory 4G \
             --executor-memory 4G \
             stream/run_stream.py \
             $KAFKACONFIGFILE $SCHEMAFILE2 $STREAMCONFIGFILE $MYSQLCONFIGFILE