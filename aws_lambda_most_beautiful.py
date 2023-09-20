import json
import os
import requests
from bs4 import BeautifulSoup
import boto3
from datetime import datetime


def lambda_handler(event, context):

    
    # WEB REQUEST
    URL = "https://www.cntraveler.com/galleries/2015-11-27/the-50-most-beautiful-places-in-the-world"
    HEADER = ({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36', 'Accept-Language': 'en-US, en; q=0.5'})
    
    # WEBPAGE
    webpage = requests.get(URL, headers=HEADER)
    try:
        if webpage.status_code == 200:
            soup = BeautifulSoup(webpage.content, "html.parser")
    except:
        print("Unable to crawl the website for data !")
    
    # CLASSIFYING THE DATA
    places = []
    places_list = soup.find_all("span", attrs = {'class': 'GallerySlideCaptionHedText-iqjOmM jwPuvZ'})
    places_list.pop(4)
    for place in range(len(places_list)):
        places.append(str(places_list[place].text))
        
    
    # UPLOADING THE DATA TO S3
    client = boto3.client('s3')
    filename = "most_beautiful_places_raw_" + str(datetime.now()) + ".json"
    try:
        client.put_object(
            Bucket = "geolocation-etl-project",
            Key = "raw_data/to-be-processed/" + filename,
            Body = json.dumps(places)
        )
        print("Data uploaded on S3 successfully.")
    except Exception:
        print("Data failed to upload on S3.")
