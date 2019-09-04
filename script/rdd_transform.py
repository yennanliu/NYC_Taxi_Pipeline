
def get_max_Total_Amt(df):
    """
    df_yellow  = load_s3_yellowtrip_data()
    df_yellow.rdd.map(
    lambda x : Row(
        vendor_name = x['vendor_name'], 
        Total_Amt = x['Total_Amt'] )
    ).max(key = lambda x : x['Total_Amt'])
    """
    max_Total_Amt = (df.rdd.map(
    lambda x : Row(vendor_name = x['vendor_name'], 
                   Total_Amt = x['Total_Amt'] ))
    .max(key = lambda x : x['Total_Amt']))
    return max_Total_Amt


def get_group_max_Total_Amt(rdd):
    """
    input  : yellow taxi RDD
    output : max Total_Amt for each vendor_name group 
    """
    filter_rdd = rdd.map(lambda x : (float(x[19]), x[2]))
    grouped_sum = filter_rdd.map(lambda x: ((x[1]), x[0])).reduceByKey(add).collect()
    grouped_max = filter_rdd.map(lambda x: ((x[1]), x[0])).reduceByKey(max).collect()
    return grouped_max, grouped_sum 
