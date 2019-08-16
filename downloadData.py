import os 
import pandas as pd 

def pre_steup():
    output_dir='./data'
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

def prepare_data_url(start_month,end_month):
    urls=[]
    months = pd.date_range(start_month, end_month, freq='MS').strftime("%Y-%m").tolist()
    travels = ['yellow', 'green']
    for month in months:
        for travel in travels:
            url='https://s3.amazonaws.com/nyc-tlc/trip+data/{}_tripdata_{}.csv'.format(travel, month)
            print (url)
            urls.append(url)
    return urls


