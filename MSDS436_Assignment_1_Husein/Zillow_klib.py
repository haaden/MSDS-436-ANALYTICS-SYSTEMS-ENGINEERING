#***********************************************
# File: zillow_klib.py
# Desc: Python script to work with zillow house sale data for chicago
# Purpose: Python script to get the list of files containing information
#          about lattitude, longitude, size, url, street, city, zipcode, price from zillow
#          1. webscrape the website
#          2. get data in a json list
#          3. use klib to clean and optimze data tyoes
#          5. Upload files in csv format on AWS S3
# Auth: Husein Adenwala
# Date: 9/29/2022

#************************************************/


import requests
import json
import pandas as pd
import numpy as np
import os
import logging
from sh import gunzip
from datetime import datetime
from argparse import ArgumentParser
from argparse import RawTextHelpFormatter
import awsapi
from io import StringIO
from bs4 import BeautifulSoup
import klib

# set up process to load the data to S3 bucker


def main():
    parser = ArgumentParser(formatter_class=RawTextHelpFormatter, prog='USdata.py', description='uploads US population data in csv format to AWS. \n ')

    parser.add_argument('-b', '--bucket_name', dest='bucket_name', help='AWS bucket to which files to be uploaded', required=True)
    parser.add_argument('-o', '--object_name', dest='object_name', help='AWS directory to which files to be uploaded')
    parser.add_argument('-d', '--dir', dest='dir', help='Local directory for downloading files & uploading to AWS', required=False)
    parser.add_argument('-l', '--log_lvl', dest='log_lvl', choices=['NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Logging level to create logs', default='WARNING')

    args = parser.parse_args()

    bucket_name = args.bucket_name
    object_name = args.object_name
    dir         = args.dir
    log_lvl     = args.log_lvl

    if log_lvl.upper()== 'NOTSET':
        log_level = logging.NOTSET
    elif log_lvl.upper()== 'DEBUG':
        log_level = logging.DEBUG
    elif log_lvl.upper()== 'INFO':
        log_level = logging.INFO
    elif log_lvl.upper()== 'WARNING':
        log_level = logging.WARNING
    elif log_lvl.upper()== 'ERROR':
        log_level = logging.ERROR
    elif log_lvl.upper()== 'CRITICAL':
        log_level = logging.CRITICAL

    # Set up logging
    logging.basicConfig(level=log_level,
                        format='%(levelname)s: %(asctime)s: %(message)s')

    # Add '/' to directory strings if not already present at the end
    if object_name and object_name[-1]!='/':
        object_name += '/'

    if dir[-1]!='/':
        dir += '/'

    # Create dir if not already exists
    if not os.path.exists(dir):
        os.makedirs(dir)


    # web scarpe data from zillow using beautifulsoup


    url = 'https://www.zillow.com/search/GetSearchPageState.htm'

    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'upgrade-insecure-requests': '1',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'
    }

    houses = []
    for page in range(1, 10):
        params = {
            "searchQueryState": json.dumps({
                "pagination": {"currentPage": page},
                "usersSearchTerm": "35216",
                "mapBounds": {
                    "west":-90.21762223828125,
                              "east":-85.24630876171875,
                              "south":41.54584411306934,
                              "north":42.12088874474264
                },
                "mapZoom": 11,
                "regionSelection": [
                    {
                        "regionId": 17426, "regionType": 6
                    }
                ],
                "isMapVisible": True,
                "filterState": {
                    "isAllHomes": {
                        "value": True
                    },
                    "sortSelection": {
                        "value": "globalrelevanceex"
                    }
                },
                "isListVisible": True
            }),
            "wants": json.dumps(
                {
                    "cat1": ["listResults", "mapResults"],
                    "cat2": ["total"]
                }
            ),
            "requestId": 3
        }

        # send request
        page = requests.get(url, headers=headers, params=params)

        # get json data
        json_data = page.json()

        # loop via data
        for house in json_data['cat1']['searchResults']['listResults']:
            houses.append(house)




    # save data in pandas data frame and clean the data
    # select key data and create data frame
    results =[]

    for house in houses:
        try:


            results.append({
                'latitude': house['latLong']['latitude'],
                'longitude': house['latLong']['longitude'],
                'floorSize': house['area'],
                'street' : house['addressStreet'],
                'city':  house['addressCity'],
                'state': house['addressState'],
                'zipcode': house['addressZipcode'],
                'url': house['detailUrl'],
                'price': house['price']
            })
        except KeyError:
            pass

    zillow = pd.DataFrame(results)
    zillow =  klib.data_cleaning(zillow)




    zillow_chicago= zillow.head(50)

    date = datetime.now().strftime("%Y%m%d")

    # Upload files to AWS S3

    #table["Text"] = table["Text"].apply(remove_non_ascii)

    csv_buffer = StringIO()
    zillow_chicago.to_csv(csv_buffer,index=False, date_format= '%Y-%m-%d %H:%M:%S')

    awsapi.upload_csv(bucket_name =bucket_name,  key= '{o}'.format(o=object_name)+'{d}'.format(d=date)+'/'+'zillow_chicago_klib.csv',  body=csv_buffer.getvalue())


        #os.remove(uf)


if __name__ == "__main__":
    main()
