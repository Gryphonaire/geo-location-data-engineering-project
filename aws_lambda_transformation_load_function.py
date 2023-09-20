import json
import os
from geopy.geocoders import Nominatim
from functools import partial
import geopy
from geopy.extra.rate_limiter import RateLimiter
from datetime import datetime
from opencage.geocoder import OpenCageGeocode
from pprint import pprint
import boto3
import pandas as pd
from io import StringIO
import time


geo_key = os.environ.get('geo_key')
geolocator = Nominatim(user_agent="geocode_beautiful_locations_from_the_world")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=3, max_retries=5)
geocoder = OpenCageGeocode(geo_key)
s3 = boto3.client('s3')
bucket = "geolocation-etl-project"

def most_beautiful(data):
    
    most_beautiful_list = []
    
    for i in range(20):
        
        # GETTING RAW DATA
        try:
            location_description = geocoder.geocode(data[i])
        except Exception:
            print(str(i) + " " + "No data found for location: " + data[i])
            continue
        
        # GETTING ADDRESS VALUES
        latitude = str(location_description[0]['geometry']['lat'])
        longitude = str(location_description[0]['geometry']['lng'])
        loc_title = data[i].split(",")[0]

        
        try:
            city = location_description[0]['components']['city']
        except Exception:
            try:
                city = location_description[0]['components']['town']
            except Exception:
                try:
                    city = location_description[0]['components']['state']
                except Exception:
                    city = "NULL"
                    
        country = location_description[0]['components']['country']
        country_code = location_description[0]['components']['country_code']
        continent = location_description[0]['components']['continent']
        
        # GETTING POSTCODE
        lat_long = str(latitude) + ","+ str(longitude) 
        location2 = geolocator.reverse(lat_long, language="en")
        try:
            postcode = location2.raw['address']['postcode'].replace(" ","")
        except:
            postcode = "NULL"
            
            
        # ADDING VARIABLES TO DICTIONARY & LISTS
        address_dict = {'Location': loc_title, 'City/State': city, 'Country': country, 'Country Code':country_code.upper(), 'Continent': continent, 'Latitude': str(round(float(latitude),2)), 'Longitude': str(round(float(longitude),2)), 'Postcode': postcode}
        most_beautiful_list.append(address_dict)
    
    return most_beautiful_list
    
    
    
def most_haunted(data):
    
    most_haunted_list = []
    
    for i in range(20):
        
        # SEPERATING TITLE AND LOCATION
        title_list = []
        location_list = []
        counter = 0
        place_list = []
        
        place_list = data[i].strip().split(" ")
        for x in range(len(place_list)):
            if place_list[x] == "in":
                counter = 1
                continue
            elif place_list[x] != "in" and counter < 1:
                title_list.append(place_list[x])
            elif place_list[x] != "in" and counter == 1:
                location_list.append(place_list[x])
        seperator = " "
        title = seperator.join(title_list)
        location = seperator.join(location_list)
        
        # GETTING RAW DATA
        try:
            location_description = geocoder.geocode(location)
        except Exception:
            print(str(i) + " " + "No data found for location: " + location)
            continue
        
        # GETTING ADDRESS VALUES
        try:
            city = location_description[0]['components']['city']
        except Exception:
            try:
                city = location_description[0]['components']['town']
            except Exception:
                try:
                    city = location_description[0]['components']['state']
                except Exception:
                    city = "NULL"
        country = location_description[0]['components']['country']
        country_code = location_description[0]['components']['country_code']
        continent = location_description[0]['components']['continent']
        latitude = str(location_description[0]['geometry']['lat'])
        longitude = str(location_description[0]['geometry']['lng'])
        
        #GETTING POSTCODE
        lat_long = str(latitude) + ","+ str(longitude) 
        location2 = geolocator.reverse(lat_long, language="en")
        try:
            postcode = location2.raw['address']['postcode'].replace(" ","")
        except:
            postcode = "NULL"
        
        # ADDING VARIABLES TO DICTIONARY
        address_dict = {'Location': title, 'City/State': city, 'Country': country, 'Country Code':country_code.upper(), 'Continent': continent, 'Latitude': str(round(float(latitude),2)), 'Longitude': str(round(float(longitude),2)), 'Postcode': postcode}
        most_haunted_list.append(address_dict)

    return most_haunted_list
    
    
def most_religious(data):
    
    most_religious_list = []
    
    for i in range(21):
        
        # GETTING RAW DATA
        try:
            location_description = geocoder.geocode(data[i])
        except Exception:
            print(str(i) + " " + "No data found for location: " + data[i])
            continue
        
        # GETTING ADDRESS VALUES
        loc_title = data[i].split(",")[0]
        latitude = str(location_description[0]['geometry']['lat'])
        longitude = str(location_description[0]['geometry']['lng'])
        try:
            city = location_description[0]['components']['city']
        except Exception:
            try:
                city = location_description[0]['components']['town']
            except Exception:
                try:
                    city = location_description[0]['components']['state']
                except Exception:
                    city = "NULL"
        country = location_description[0]['components']['country']
        country_code = location_description[0]['components']['country_code']
        continent = location_description[0]['components']['continent']
        visitors = data[i+21]
        
        #GETTING POSTCODE
        lat_long = str(latitude) + ","+ str(longitude) 
        location2 = geolocator.reverse(lat_long, language="en")
        try:
            postcode = location2.raw['address']['postcode'].replace(" ","")
        except:
            postcode = "NULL"
            
        # ADDING VARIABLES TO DICTIONARY
        address_dict = {'Location': loc_title, 'City/State': city, 'Country': country, 'Country Code':country_code.upper(), 'Continent': continent, 'Latitude': str(round(float(latitude),2)), 'Longitude': str(round(float(longitude),2)), 'Postcode': postcode, 'Visitors (in Millions)': visitors}
        most_religious_list.append(address_dict)
    
    return most_religious_list
    
    
    
def most_toured(data):
    
    most_toured_list = []
    
    for i in range(int(len(data)/2)):
        
        # GETTING RAW DATA
        try:
         location_description = geocoder.geocode(data[i])
        except Exception:
         print(str(i) + " " + "No data found for location: " + str(data[i]))
         continue
        
        # GETTING ADDRESS COMPONENTS
        city = data[i]
        country = location_description[0]['components']['country']
        country_code = location_description[0]['components']['country_code']
        continent = location_description[0]['components']['continent']
        latitude = str(location_description[0]['geometry']['lat'])
        longitude = str(location_description[0]['geometry']['lng'])
        visitors = data[i+20].split(" ")[0]
        
        #GETTING POSTCODE
        lat_long = str(latitude) + ","+ str(longitude) 
        location2 = geolocator.reverse(lat_long, language="en")
        try:
         postcode = location2.raw['address']['postcode'].replace(" ","")
        except:
         postcode = "NULL"
        
        # ADDING VARIABLES TO DICTIONARY
        address_dict = {'City': city, 'Country': country, 'Country Code':country_code.upper(), 'Continent': continent, 'Latitude': str(round(float(latitude),2)), 'Longitude': str(round(float(longitude),2)), 'Postcode': postcode, 'Visitors (in Millions)': visitors}
        most_toured_list.append(address_dict)

    return most_toured_list



def most_expensive(data):
    
    most_expensive_list = []
    
    for i in range(20):
        
        # GETTING RAW DATA
        try:
            location_description = geocoder.geocode(data[i])
        except Exception:
            print(str(i) + " " + "No data found for location: " + data[i])
            continue
        
        # GETTING ADDRESS VALUES
        latitude = str(location_description[0]['geometry']['lat'])
        longitude = str(location_description[0]['geometry']['lng'])
        city = data[i]
        country = location_description[0]['components']['country']
        country_code = location_description[0]['components']['country_code']
        continent = location_description[0]['components']['continent']
        
        #GETTING POSTCODE
        lat_long = str(latitude) + ","+ str(longitude) 
        location2 = geolocator.reverse(lat_long, language="en")
        try:
            postcode = location2.raw['address']['postcode'].replace(" ","")
        except:
            postcode = "NULL"
            
            
        # ADDING VARIABLES TO DICTIONARY
        address_dict = {'City': city, 'Country': country, 'Country Code':country_code.upper(), 'Continent': continent, 'Latitude': str(round(float(latitude),2)), 'Longitude': str(round(float(longitude),2)), 'Postcode': postcode}
        most_expensive_list.append(address_dict)
    
    return most_expensive_list
    
    
    
def upload_transformed_data(list,name):
    
    # CONVERTING THE LIST TO DATAFRAME
    name_df = pd.DataFrame.from_dict(list)
    
    # NAMING THE FILE
    name_key = "transformed_data/" + name + "/" + name + "_places_" + str(datetime.now()) + ".csv"
    name_buffer = StringIO()
    name_df.to_csv(name_buffer, index=False)
    name_content = name_buffer.getvalue()
    
    #UPLOADING THE DATA
    try:
        s3.put_object(Bucket = bucket, Key = name_key, Body = name_content)
        print("Success: Transformed Data uploaded successfully.")
    except:
        print("Failure: Transformed Data failed to upload to S3.")
    time.sleep(2)
    



def lambda_handler(event, context):
    
    key = "raw_data/to-be-processed/"
    
    geocode_keys = []
    for file in (s3.list_objects(Bucket = bucket, Prefix = key)['Contents']):
        file_key = file['Key']
        
        if file_key.split(".")[-1] == "json":
            response = s3.get_object(Bucket = bucket, Key = file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            geocode_keys.append(file_key)
            
            if "most_beautiful" in file_key:
                most_beautiful_list = most_beautiful(jsonObject)
                time.sleep(2)
                upload_transformed_data(most_beautiful_list, "most_beautiful")
                
            elif "most_expensive" in file_key:
                most_expensive_list = most_expensive(jsonObject)
                time.sleep(2)
                upload_transformed_data(most_expensive_list, "most_expensive")
            
            elif "most_haunted" in file_key:
                most_haunted_list = most_haunted(jsonObject)
                time.sleep(2)
                upload_transformed_data(most_haunted_list, "most_haunted")
            
            elif "most_religious" in file_key:
                most_religious_list = most_religious(jsonObject)
                time.sleep(2)
                upload_transformed_data(most_religious_list, "most_religious")
            
            
            elif "most_toured" in file_key:
                most_toured_list = most_toured(jsonObject)
                time.sleep(2)
                upload_transformed_data(most_toured_list, "most_toured")
            
            else:
                print("This data file doesn't match any given template: " + file_key)
                
    # MOVING THE JSON FILE FROM 'raw_data/to_be_processed/' to 'raw_data/processed/'
    s3_resource = boto3.resource('s3')
    for key in geocode_keys:
        copy_source = {
            "Bucket" : bucket,
            "Key" : key
        }
        s3_resource.meta.client.copy(copy_source, bucket, 'raw_data/processed/'+ key.split('/')[-1])
        s3_resource.Object(bucket,key).delete()
    
