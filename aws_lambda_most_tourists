import json
import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import boto3

def lambda_handler(event, context):
    
    # GETTING ENVIRONMENT VARIABLE 
    geo_key = os.environ.get('geo_key')
    
    # WEB REQUEST
    URL = "https://travelness.com/most-visited-cities-in-the-world"
    HEADER = ({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36', 'Accept-Language': 'en-US, en; q=0.5'})
    
    # WEBPAGE
    webpage = requests.get(URL, headers=HEADER)
    
    try:
        if webpage.status_code == 200:
            soup = BeautifulSoup(webpage.content, "html.parser")
    except:
        print("Unable to crawl the website for data !")
    
    # CLASSIFYING THE DATA
    data_list = soup.find_all("td", attrs = {'class': 'has-text-align-left'})
    title_list = []
    visitor_list = []
    for data in range(len(data_list)):
        if data % 3 == 1:
            title_list.append(str(data_list[data].text))
        elif data % 3 ==2:
            visitor_list.append(str(data_list[data].text))
            
    final_data_list = title_list + visitor_list
    
    # UPLOADING THE DATA TO S3
    client = boto3.client('s3')
    filename = "most_toured_places_raw_" + str(datetime.now()) + ".json"
    try:
        client.put_object(
            Bucket = "geolocation-etl-project",
            Key = "raw_data/to-be-processed/" + filename,
            Body = json.dumps(final_data_list)
        )
        print("Data uploaded on S3 successfully.")
    except Exception:
        print("Data failed to upload on S3.")
