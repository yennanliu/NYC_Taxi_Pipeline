import numpy
import pyspark
import pyspark.sql
from datetime import datetime, timedelta

#year, month, bucket, folder = sys.argv[1:5]
#prefix = "s3a://{}/{}/".format(bucket, folder)
#filename = "trip_data_{}_{}{}.csv"
filename="s3a://nyc-tlc/trip+data/green_tripdata_2019-01.csv"

sc = pyspark.SparkContext.getOrCreate()
sqlcontext = pyspark.sql.SQLContext(sc)
data = sc.textFile(filename)
data2 = sqlcontext.createDataFrame(data.map(lambda line: transform(line, i)).filter(lambda x: x is not None))
data2.repartition(1).write.csv(prefix+filename.format(int(year)-7*i, month, "_s"))
print (data2)
print (data)