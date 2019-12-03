import json
import heapq
import utility
import postgres
import pyspark

class BatchProcessor:
    """
    class that load data from S3 bucket, processes it with Spark 
    and saves the result into PostgreSQL 
    """
    def __init__(self, s3_configfile, schema_configfile, psql_configfile):
        """
        class constructor that initializes the instance according to the configurations
        of the S3 bucket, raw data and PostgreSQL table
        
        :type s3_configfile:     str        path to s3 config file
        :type schema_configfile: str        path to schema config file
        :type psql_configfile:   str        path to psql config file
        """
        self.s3_config   = utility.parse_config(s3_configfile)
        self.schema      = utility.parse_config(schema_configfile)
        self.psql_config = utility.parse_config(psql_configfile)

        self.sc = pyspark.SparkContext.getOrCreate()
        self.sc.setLogLevel("ERROR")

    def read_from_s3(self):
        """
        reads files from s3 bucket defined by s3_configfile and creates Spark RDD
        """
        filenames = "s3a://{}/{}/{}".format(self.s3_config["BUCKET"],
                                            self.s3_config["FOLDER"],
                                            self.s3_config["RAW_DATA_FILE"])
        self.data = self.sc.textFile(filenames)

    def save_to_postgresql(self):
        """
        saves result of batch transformation to PostgreSQL database and adds necessary index
        """
        configs = {key: self.psql_config[key] for key in ["url", "driver", "user", "password"]}
        configs["dbtable"] = self.psql_config["dbtable_batch"]

        postgres.save_to_postgresql(self.data, pyspark.sql.SQLContext(self.sc), configs, self.psql_config["mode_batch"])
        postgres.add_index_postgresql(configs["dbtable"], self.psql_config["partitionColumn"], self.psql_config)

    def spark_transform(self):
        """
        transforms Spark RDD with raw data into RDD with cleaned data;
        adds block_id, sub_block_id and time_slot fields
        """
        schema = self.sc.broadcast(self.schema)
        self.data = (self.data
                           .map(lambda line: utility.map_schema(line, schema.value))
                           .map(utility.add_block_fields)
                           .map(utility.add_time_slot_field)
                           .map(utility.check_passengers)
                           .filter(lambda x: x is not None))

    def run(self):
        """
        executes the read from S3, transform by Spark and write to PostgreSQL database sequence
        """
        self.read_from_s3()
        self.spark_transform()
        self.save_to_postgresql()


class TaxiBatchProcessor(BatchProcessor):
    """
    class that calculates the top-n pickup spots from historical data
    """

    def spark_transform(self):
        """
        transforms Spark RDD with raw data into the RDD that contains
        top-n pickup spots for each block and time slot
        """
        BatchTransformer.spark_transform(self)

        n = self.psql_config["topntosave"]

        # calculation of top-n spots for each block and time slot
        self.data = (self.data
                        .map(lambda x: ( (x["block_id"], x["time_slot"], x["sub_block_id"]), x["passengers"] ))
                        .reduceByKey(lambda x,y: x+y)
                        .map(lambda x: ( (x[0][0], x[0][1]), [(x[0][2], x[1])] ))
                        .reduceByKey(lambda x,y: x+y)
                        .mapValues(lambda vals: heapq.nlargest(n, vals, key=lambda x: x[1]))
                        .map(lambda x: {"block_id":          x[0][0],
                                        "time_slot":         x[0][1],
                                        "subblocks_psgcnt":  x[1]}))

        self.data.persist(pyspark.StorageLevel(True, True, False, False, 3)).count()   # MEMORY_AND_DISK_3


        # recalculation of top-n, where for each key=(block_id, time_slot) top-n is calculated
        # based on top-n of (block_id, time_slot) and top-ns of (adjacent_block, time_slot+1)
        # from all adjacent blocks
        maxval = self.psql_config["upperBound"]
        self.data = (self.data
                        .map(lambda x: ( (x["block_id"], x["time_slot"]), x["subblocks_psgcnt"] ))
                        .flatMap(lambda x: [x] + [ ( (bl, (x[0][1]-1) % maxval), x[1] ) for bl in utility.get_neighboring_blocks(x[0][0]) ] )
                        .reduceByKey(lambda x,y: x+y)
                        .mapValues(lambda vals: heapq.nlargest(n, vals, key=lambda x: x[1]))
                        .map(lambda x: {"block_latid":  x[0][0][0],
                                        "block_lonid":  x[0][0][1],
                                        "time_slot":    x[0][1],
                                        "longitude":    [utility.determine_subblock_lonlat(el[0])[0] for el in x[1]],
                                        "latitude":     [utility.determine_subblock_lonlat(el[0])[1] for el in x[1]],
                                        "passengers":   [el[1] for el in x[1]] } ))

        self.data.persist(pyspark.StorageLevel(True, True, False, False, 3)).count()    # MEMORY_AND_DISK_3
