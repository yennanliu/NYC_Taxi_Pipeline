# NYC_Taxi_Pipeline


### Process 
```
                             --> Stream pre-process --> stream dataset -->  stream pipeline --> real-time report 
NYC tlc-trip-record-data ---> 

                             --> Storage to DB/csv --> batch pipeline --> historical report  

```

### Quick start 
<details>
<summary>Quick-Start-Batch-Pipeline-Manually</summary>

```bash 
$ git clone https://github.com/yennanliu/NYC_Taxi_Pipeline.git
$ cd NYC_Taxi_Pipeline
# download and upload data to s3 
# init mysql 
$ echo "create database `taxi`" | mysql -u <user_name> -p <mysql_password>
$ mysql -u <mysql_user_name> -p <mysql_password> taxi < batch/create_table.sql
$ export SPARK_HOME=/Users/$USER/spark && export PATH=$SPARK_HOME/bin:$PATH
$ spark-submit batch/batch_spark_test.py 

```
</details>
