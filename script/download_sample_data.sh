#!/bin/bash
#################################################################
# SCRIPT DOWNLOAD TAXI DATA AS SAMPLE (100 records) VIA wget 
#################################################################

MONTH_ORDINALS=("01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12")
YEAR_ORDINALS=("2009" "2010" "2011" "2012" "2013" "2014" "2015" "2016") 

export srcDataDirRootYellow=data/staging/transactional-data/yellow-taxi
export srcDataDirRootGreen=data/staging/transactional-data/green-taxi

y_urls=("https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-01.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-02.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-03.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-04.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-05.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-06.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-07.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-08.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-09.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-10.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-11.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-12.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-01.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-02.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-03.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-04.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-05.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-06.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-07.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-08.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-09.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-10.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-11.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-12.csv")

g_urls=("https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-01.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-02.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-03.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-04.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-05.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-06.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-07.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-08.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-09.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-10.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-11.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2016-12.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2017-01.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2017-02.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2017-03.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2017-04.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2017-05.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2017-06.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2017-07.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2017-08.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2017-09.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2017-10.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2017-11.csv"
        "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2017-12.csv")

download_yellow_data(){
  echo ">>> download_yellow_data"
  for url in ${y_urls[@]}; 
  do 
      echo $url
      filename="`echo $url | sed 's/trip+data/ /g' |  awk '{print $2 }'`"
      echo $filename
      #echo $srcDataDirRoot$filename
      wget $url -qO - | head -100  >> $srcDataDirRootYellow$filename
  done 
}

download_green_data(){
  echo ">>> download_green_data"
  for url in ${g_urls[@]}; 
  do 
      echo $url
      filename="`echo $url | sed 's/trip+data/ /g' |  awk '{print $2 }'`"
      echo $filename
      #echo $srcDataDirRoot$filename
      wget $url -qO - | head -100  >> $srcDataDirRootGreen$filename
  done 
}

download_yellow_data
download_green_data
