spark-submit --master spark://$SPARK_STREAM_CLUSTER_0:7077 \
             --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 \
             --jars $PWD/postgresql-42.2.2.jar \
             --py-files $AUX_FILES \
             --driver-memory 4G \
             --executor-memory 4G \
             streaming/main_stream.py \
             $KAFKACONFIGFILE $SCHEMAFILE2 $STREAMCONFIGFILE $PSQLCONFIGFILE