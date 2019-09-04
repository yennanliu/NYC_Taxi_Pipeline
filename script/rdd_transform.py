#################################################################
# SCRIPT GET TRANSFORMED RDD AND STATISTICS    
#################################################################

class RDD_Transform(object):

    def __init__(self):

        self.rdd = sc.textfile("data/yellow_tripdata_sample.csv")

    def load_rdd(self, filename):

        if not self.rdd:
            self.rdd = sc.textfile(filename)

    def get_max_Total_Amt(self, rdd):
        
        max_Total_Amt = self.rdd.map( lambda x : [x[2], x[19]]) \
                           .max(key = lambda x : x[2])
        return max_Total_Amt

    def get_group_max_Total_Amt(self, rdd):

        def add(x,y):
            return x + y 

        filter_rdd = rdd.map(lambda x : (float(x[19]), x[2]))
        grouped_sum = filter_rdd.map(lambda x: ((x[1]), x[0])).reduceByKey(add).collect()
        grouped_max = filter_rdd.map(lambda x: ((x[1]), x[0])).reduceByKey(max).collect()
        return grouped_max, grouped_sum 

    def get_timestamp_rdd(self, rdd):

        def to_timestamp(x):
            return datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
        filter_rdd = rdd.map(lambda x: x[3])

        return filter_rdd.map(to_timestamp).collect()

    def get_distance_rdd(self, rdd):

        def get_pseudo_distance(geo_array):
            x1,y1,x2,y2 = float(geo_array[0]), float(geo_array[1]), float(geo_array[2]), float(geo_array[3])
            return ((x1-x2)**2 + (y1-y2)**2)**(0.5)

        return rdd.map(lambda x : [x[6],x[7], x[10],x[11] ]) \
          .map(get_pseudo_distance).collect()



# def get_max_Total_Amt(df):
#     """
#     df_yellow  = load_s3_yellowtrip_data()
#     df_yellow.rdd.map(
#     lambda x : Row(
#         vendor_name = x['vendor_name'], 
#         Total_Amt = x['Total_Amt'] )
#     ).max(key = lambda x : x['Total_Amt'])
#     """
#     max_Total_Amt = (df.rdd.map(
#     lambda x : Row(vendor_name = x['vendor_name'], 
#                    Total_Amt = x['Total_Amt'] ))
#     .max(key = lambda x : x['Total_Amt']))
#     return max_Total_Amt
#
#
# def get_group_max_Total_Amt(rdd):
#     """
#     input  : yellow taxi RDD
#     output : max Total_Amt for each vendor_name group 
#     """
#     filter_rdd = rdd.map(lambda x : (float(x[19]), x[2]))
#     grouped_sum = filter_rdd.map(lambda x: ((x[1]), x[0])).reduceByKey(add).collect()
#     grouped_max = filter_rdd.map(lambda x: ((x[1]), x[0])).reduceByKey(max).collect()
#     return grouped_max, grouped_sum 
#
# def to_timestamp(x):
#     return datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
#
# def get_timestamp_rdd(rdd):
#     filter_rdd = rdd.map(lambda x: x[3])
#     return filter_rdd.map(to_timestamp).collect()