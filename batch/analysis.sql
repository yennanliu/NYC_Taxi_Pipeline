-- SQL to calculate supply VS demand every 5 minute by geo-hash id 

SELECT pickup.*,
       dropoff.*
FROM
  (SELECT DATE_FORMAT(MIN(Trip_Pickup_DateTime), '%d/%m/%Y %H:%i:00') AS tmstamp,
          geo_hash_id,
          COUNT(id) AS count_in_time_interval
   FROM pickup_geo_hash
   GROUP BY ROUND(UNIX_TIMESTAMP(Trip_Pickup_DateTime) / 300),
            geo_hash_id) AS pickup
INNER JOIN
  (SELECT DATE_FORMAT(MIN(Trip_Dropoff_DateTime), '%d/%m/%Y %H:%i:00') AS tmstamp,
          geo_hash_id,
          COUNT(id) AS count_in_time_interval
   FROM dropoff_geo_hash
   GROUP BY ROUND(UNIX_TIMESTAMP(Trip_Dropoff_DateTime) / 300),
            geo_hash_id) AS dropoff ON pickup.tmstamp = dropoff.tmstamp
AND pickup.geo_hash_id = dropoff.geo_hash_id;

