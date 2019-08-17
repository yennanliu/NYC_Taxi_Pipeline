# access s3 via spark  shell
export AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>
export AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
export SPARK_HOME=/Users/$USER/spark
export PYSPARK_DRIVER_PYTHON=ipython
# run pysprak via ipython 
pyspark 

### inside spark shell 
# import os
# import pyspark
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.7.4,org.apache.hadoop:hadoop-aws:2.7.6 pyspark-shell'
# from pyspark.sql import SQLContext
# from pyspark import SparkContext
# #sc = SparkContext()
# sqlContext = SQLContext(sc)
# filename = "s3a://nyctaxitrip/green_trip/green_tripdata_2019-01.csv"
# sc = pyspark.SparkContext.getOrCreate()
# sqlcontext = pyspark.sql.SQLContext(sc)
# data = sc.textFile(filename)
# data.count()