import json
import os
import requests
import re
from bs4 import BeautifulSoup
from datetime import datetime
import boto3

def lambda_handler(event, context):
    
    # WEB REQUEST
    URL = "https://www.travelandleisure.com/attractions/worlds-most-visited-sacred-sites"
    HEADER = ({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36', 'Accept-Language': 'en-US, en; q=0.5'})
    
    # WEBPAGE
    webpage = requests.get(URL, headers=HEADER)
    
    try:
        if webpage.status_code == 200:
            soup = BeautifulSoup(webpage.content, "html.parser")
    except:
        print("Unable to crawl the website for data !")
    
    # CLASSIFYING THE DATA
    title_list = soup.find_all("span", attrs = {'class': 'mntl-sc-block-heading__text'})
    visitors_list = soup.find_all("p", attrs = {'class': 'comp mntl-sc-block mntl-sc-block-html'})
    
    new_title_list = []
    for i in range(len(title_list)):
        temp_title = title_list[i].text.strip()
        title = re.sub(r"No\. \b([1-9]|[12][0-9])\b\s", "", temp_title)
        new_title_list.append(str(title))
    
    new_visitors_list = []
    sample_word = "Annual Visitors:"
    for i in range(len(visitors_list)):
        if sample_word in visitors_list[i].text:
            visitor = visitors_list[i].text.strip("\n").split(":")[1].strip().split(" ")[0].split("–")[0]
            new_visitors_list.append(str(visitor))
    
    final_title_list = new_title_list + new_visitors_list
    
    # UPLOADING THE DATA TO S3
    client = boto3.client('s3')
    filename = "most_religious_places_raw_" + str(datetime.now()) + ".json"
    try:
        client.put_object(
            Bucket = "geolocation-etl-project",
            Key = "raw_data/to-be-processed/" + filename,
            Body = json.dumps(final_title_list)
        )
        print("Data uploaded on S3 successfully.")
    except Exception:
        print("Data failed to upload on S3.")
