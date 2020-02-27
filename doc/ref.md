## Ref

### Documentation 
- https://registry.opendata.aws/nyc-tlc-trip-records-pds/

### Idea 
- https://github.com/aws-samples/amazon-kinesis-analytics-taxi-consumer

### Dataset 
- Main dataset
	- https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Lookup/Reference dataset
	- trip_month_lookup.csv
	- rate_code_lookup.csv
	- payment_type_lookup.csv
	- trip_type_lookup.csv
	- vendor_lookup.csv
	- https://github.com/microsoft/Azure-Databricks-NYC-Taxi-Workshop/issues/12

### Reference code 
- https://github.com/acoullandreau/nyc_taxi_trips - init sql 
- https://github.com/lingyielia/YellowTaxi - geo hash
- https://github.com/AndreyBozhko/TaxiOptimizer  - pipeline 
- https://towardsdatascience.com/putting-ml-in-production-i-using-apache-kafka-in-python-ce06b3a395c8 - Kafka
- https://engineeringblog.yelp.com/2016/08/streaming-mysql-tables-in-real-time-to-kafka.html - Streaming mysql to Kafka 
- https://www.confluent.io/blog/ksql-in-action-enriching-csv-events-with-data-from-rdbms-into-AWS/ - Streaming csv to Kafka

### Framework 
- Lambda VS Kappa
	- https://towardsdatascience.com/a-brief-introduction-to-two-data-processing-architectures-lambda-and-kappa-for-big-data-4f35c28005bb
