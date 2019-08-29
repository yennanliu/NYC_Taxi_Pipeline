
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
