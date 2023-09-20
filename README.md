# Geo-Location Data Engineering

## Description
In this project, we will build an ETL pipeline using GeoPY and OpenCage APIs on AWS. The pipeline will use python to crawl some websites, transform the information retrived and then upload it to the database on AWS.

## Architecture 
![Architecture](https://github.com/aman-tripathi-01/geo-location-data-engineering-project/assets/31034814/15b37927-f06d-445a-8e56-96c25b9a38d0)

## Dataset
We used python to crawl the 5 websites mentioned below to extract the data:
1. [The 51 Most Beautiful Places in the World](https://www.cntraveler.com/galleries/2015-11-27/the-50-most-beautiful-places-in-the-world)
2. [2023 Top 20 Most Expensive Cities in the World](https://www.caproasia.com/2023/06/21/2023-top-20-most-expensive-cities-in-the-world-top-10-cities-are-new-york-hong-kong-geneva-london-singapore-zurich-san-francisco-tel-aviv-seoul-tokyo/)
3. [35 Most Haunted Places in the World](https://www.travelandleisure.com/holiday-travel/halloween/most-haunted-places-in-the-world)
4. [World's Most-Visited Sacred Sites](https://www.travelandleisure.com/attractions/worlds-most-visited-sacred-sites)
5. [20 Most Visited Cities in the World](https://travelness.com/most-visited-cities-in-the-world)

## APIs
1. **GeoPy:** This is used to locate the coordinates of addresses, cities, countries, and landmarks across the globe using third-party geocoders and other data sources - [GeoPy API](https://geopy.readthedocs.io/en/latest/)
2. **Opencage Geocoding:** This APIs provides worldwide, reverse (latitude/longitude to text) and forward (text to latitude/longitude) geocoding based on open data via a REST API - [OpenCage API](https://opencagedata.com/api)

## AWS Services Used:
1. **Amazon S3:** Amazon S3 is an object storage service that provides manufacturing scalability, data availability, security, and performance.
2. **AWS Lambda:** Lambda is a computing service that allows programmers to run code without creating or managing servers.
3. **Glue Crawler:** Crawlers are used to populate the tables in AWS Glue Data Catalog
4. **AWS Athena:** Athena is an interactive query service for S3 in which there is no need to load data it stays in S3.
5. **Amazon EventBridge:** Eventbridge is used to schedule tasks and events.
6. **AWS IAM:** This is nothing but identity and access management which enables us to manage access to AWS services and resources securely.

## Python Packages
```
Pandas
GeoPy
re
OpenCage
requests
BeautifulSoup4
Boto3
io
```
## Project Execution Flow
Lambda trigger(runs on 15th of every month) -> Python code (defined in AWS Lambda) crawls the website -> AWS Lambda function also extacts the necessary data -> Data is uploaded to S3 -> Lambda trigger to load the data (runs whenever new data is added to S3) -> Transformation AWS Lambda function is executed -> Processed data is uploaded to S3 -> Glue Crawler converts data to tables in the database -> Athena can be used for insights from the data.
