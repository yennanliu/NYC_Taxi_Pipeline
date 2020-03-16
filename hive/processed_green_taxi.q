CREATE EXTERNAL TABLE `processed_green_taxi`(
  `taxi_type` string, 
  `vendor_id` string, 
  `pickup_datetime` timestamp, 
  `dropoff_datetime` timestamp, 
  `store_and_fwd_flag` string, 
  `rate_code_id` string,
  `pickup_location_id` double, 
  `dropoff_location_id` double, 
  `pickup_longitude` double, 
  `pickup_latitude` double, 
  `dropoff_longitude` double,
  `dropoff_latitude` double,
  `passenger_count` double,
  `trip_distance` double, 
  `fare_amount` double,
  `extra` double,
  `mta_tax` double,
  `tip_amount` double,
  `tolls_amount` double,
  `improvement_surcharge` double,
  `total_amount` double,
  `ehail_fee` double,
  `payment_type` string,
  `trip_type` string)

ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS
INPUTFORMAT
  'com.amazonaws.emr.s3select.hive.S3SelectableTextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://nyc-tlc-taxi/data/processed/green-taxi/'
TBLPROPERTIES (
  "s3select.format" = "csv",
  "s3select.headerInfo" = "ignore"
);