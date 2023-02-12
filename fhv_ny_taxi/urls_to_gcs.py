# use proper service account credentials
# gcloud auth list
# gcloud config set account 588486039920-compute@developer.gserviceaccount.com

import time
import pandas as pd
import requests
from google.cloud import storage

# Create a GCP client
client = storage.Client()

# Define the bucket name
bucket_name = 'gcpzoomcamp'

# Create a bucket object
bucket = client.bucket(bucket_name)

# List of URLs with CSV files
year = 2019
url_list = [ f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month:02}.csv.gz' for month in range(1,12)  ]

# or one by one if errors out in the process
url_list = [
    #'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz',
    # 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-02.csv.gz',
    # 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-03.csv.gz',
    # 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-04.csv.gz',
    # 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-05.csv.gz',
    # 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-06.csv.gz',
    # 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-07.csv.gz',
    # 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-08.csv.gz',
    # 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-09.csv.gz',
    # 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-10.csv.gz',
    # 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-11.csv.gz',
     'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-12.csv.gz',
]
# taxi zone file location
# url_list = ['https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv']

# Iterate through the list of URLs
for url in url_list:
    # Download the CSV file
    response = requests.get(url)
    # Get the file name from the URL
    file_name = url.split("/")[-1]
    # Create a blob object
    blob = bucket.blob("fhv/" + file_name)
    # Check if the file already exists in the bucket
    if blob.exists():
        print(f'{file_name} already exists in {bucket_name}. Skipping upload...')
        continue
    # upload in one go - errors out from time to time
    blob.upload_from_string(response.content)
    print(f'{file_name} has been uploaded to {bucket_name}')

