#!/bin/bash
# Change S3_BUCKET to your S3 bucket
export S3_BUCKET="s3://nyctaxitrip"
# Change S3_BASE_FOLDER to your base folder, but should be nyc-taxi if you are following the tutorial
export S3_BASE_FOLDER="nyc-taxi"
# Change AWS_ACCESS_KEY_ID to your key
export AWS_ACCESS_KEY_ID=MY_ACCESS_KEY
# ChangeAWS_SECRET_ACCESS_KEY to your secret key
export AWS_SECRET_ACCESS_KEY=MY_SECRET_KEY
# Change S3_BASE_FOLDER to your base folder.
export AWS_DEFAULT_PROFILE=yenliu
export AWS_DEFAULT_REGION=us-east-1
aws configure --profile $AWS_DEFAULT_PROFILE 
# do not change URL_ROOT
URL_ROOT="https://s3.amazonaws.com/nyc-tlc/trip+data/"
# modify if needed for smaller subsets or add green and fhv for additional cab_types
MONTH_ORDINALS=("01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12")
YEAR_ORDINALS=("2009" "2010" "2011" "2012" "2013" "2014" "2015" "2016") 
CAB_TYPES=("yellow")
# leave as empty
FILE_NAME=""
S3_FOLDER=""
S3_SUBFOLDER=""
for name in ${CAB_TYPES[@]}
  do
    if [ $name == "yellow" ]; then
      S3_FOLDER="yellow_tripdata"
      YEARS=${YEAR_ORDINALS[@]}
    fi
    for yy in ${YEARS[@]}
      do
        MONTHS=${MONTH_ORDINALS[@]}
        for mm in ${MONTHS[@]}
        do
          FILE_NAME=${name}_tripdata_${yy}-${mm}
          # get the csv file 
          curl -S -O "${URL_ROOT}${FILE_NAME}.csv" && echo "done! curl ${FILE_NAME}.csv" &
          wait
          # tarball the file
          tar -cvzf "${FILE_NAME}.tar.gz" "${FILE_NAME}.csv"  && echo "done! tar ${FILE_NAME}.tar.gz" &
          wait
          # upload to AWS S3 the gz file
          if [[ $name == "yellow"  &&  $yy == "2015" ]]; then
            S3_SUBFOLDER="201501-201606"
          elif [[ $name == "yellow"  &&  $yy == "2016" &&  $mm == "01" ]]; then
            S3_SUBFOLDER="201501-201606"
          elif [[ $name == "yellow"  &&  $yy == "2016" &&  $mm == "02" ]]; then
            S3_SUBFOLDER="201501-201606"
          elif [[ $name == "yellow"  &&  $yy == "2016" &&  $mm == "03" ]]; then
            S3_SUBFOLDER="201501-201606"
          elif [[ $name == "yellow"  &&  $yy == "2016" &&  $mm == "04" ]]; then
            S3_SUBFOLDER="201501-201606"
          elif [[ $name == "yellow"  &&  $yy == "2016" &&  $mm == "05" ]]; then
            S3_SUBFOLDER="201501-201606"
          elif [[ $name == "yellow"  &&  $yy == "2016" &&  $mm == "06" ]]; then
            S3_SUBFOLDER="201501-201606"
          elif [[ $name == "yellow"  &&  $yy == "2016" &&  $mm == "07" ]]; then
            S3_SUBFOLDER="201607-201612"
          elif [[ $name == "yellow"  &&  $yy == "2016" &&  $mm == "08" ]]; then
            S3_SUBFOLDER="201607-201612"
          elif [[ $name == "yellow"  &&  $yy == "2016" &&  $mm == "09" ]]; then
            S3_SUBFOLDER="201607-201612"
          elif [[ $name == "yellow"  &&  $yy == "2016" &&  $mm == "10" ]]; then
            S3_SUBFOLDER="201607-201612"
          elif [[ $name == "yellow"  &&  $yy == "2016" &&  $mm == "11" ]]; then
            S3_SUBFOLDER="201607-201612"
          elif [[ $name == "yellow"  &&  $yy == "2016" &&  $mm == "12" ]]; then
            S3_SUBFOLDER="201607-201612"
          else
            S3_SUBFOLDER="200901-201412"
          fi   
          if [ $name == "yellow" ]; then
            aws s3 cp ${FILE_NAME}.tar.gz ${S3_BUCKET}/${S3_BASE_FOLDER}/${S3_FOLDER}/${S3_SUBFOLDER}/ --profile $AWS_DEFAULT_PROFILE && echo "done! aws s3 cp ${FILE_NAME}.tar.gz" &  
          fi
          wait
          #rm the cv files
          rm -f "${FILE_NAME}.csv" && echo "done! rm -f ${FILE_NAME}.csv" &
          wait
          #rm the gz files
          rm -f "${FILE_NAME}.tar.gz"  && echo "done! rm -f ${FILE_NAME}.tar.gz" &
          wait
        done
      done
  done