-- Create db in mysql
CREATE DATABASE IF NOT EXISTS taxi;

-- Create a table with yellow taxi trip 
use taxi; 
DROP TABLE IF EXISTS yellow_trip;
CREATE TABLE yellow_trip  (
        id INT NOT NULL AUTO_INCREMENT,
        vendor_name CHAR(10),
        Trip_Pickup_DateTime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        Trip_Dropoff_DateTime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        Passenger_Count INTEGER NULL,
        Trip_Distance FLOAT NOT NULL,
        Start_Lon FLOAT NULL,
        Start_Lat FLOAT NULL,
        Rate_Code CHAR(10) NULL,
        store_and_forward CHAR(10) NULL,
        End_Lon FLOAT  NULL,
        End_Lat FLOAT  NULL,
        Payment_Type CHAR(10)  NULL,
        Fare_Amt FLOAT  NULL,
        surcharge FLOAT  NULL,
        mta_tax FLOAT NULL,
        Tip_Amt FLOAT NULL,
        Tolls_Amt FLOAT NULL,
        Total_Amt FLOAT NULL,
        PRIMARY KEY (id)
        );