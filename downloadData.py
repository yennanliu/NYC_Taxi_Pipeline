import os 
import csv
import requests
import pandas as pd 
from tqdm import tqdm

def pre_steup():
    output_dir='./data'
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

def prepare_data_url(start_month,end_month):
    urls=[]
    months = pd.date_range(start_month, end_month, freq='MS').strftime("%Y-%m").tolist()
    #travels = ['yellow', 'green']
    travels = ['yellow']
    for month in months:
        for travel in travels:
            url='https://s3.amazonaws.com/nyc-tlc/trip+data/{}_tripdata_{}.csv'.format(travel, month)
            print (url)
            urls.append(url)
    return urls

def Download_2_csv(data_urls):
    for data_url in data_urls:
        print ('>>> downloading ', data_url)
        file_name = data_url.split('/')[-1]
        download = requests.get('/data'+ data_url)
        count = 0 
        with open(file_name, 'w') as temp_file:
            while count < 100:
                temp_file.writelines(str(download.content))
                count += 1 

if __name__ == '__main__':
    pre_steup()
    urls = prepare_data_url('2019-01','2019-01')
    Download_2_csv(urls)
