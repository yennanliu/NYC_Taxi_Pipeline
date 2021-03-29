CREATE EXTERNAL TABLE IF NOT EXISTS nyc_taxi_tlc.taxi_trip (
  `vendorid` string,
  `tpep_pickup_datetime` string,
  `tpep_dropoff_datetime` string,
  `passenger_count` string,
  `trip_distance` string,
  `ratecodeid` string,
  `store_and_fwd_flag` string,
  `pulocationid` string,
  `dolocationid` string,
  `payment_type` string,
  `fare_amount` string,
  `extra` string,
  `mta_tax` string,
  `tip_amount` string,
  `tolls_amount` string,
  `improvement_surcharge` string,
  `total_amount` string,
  `congestion_surcharge` string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://nyc-tlc-taxi/tripdata/'
TBLPROPERTIES ('has_encrypted_data'='false');