#***********************************************
# File: USdata.py
# Desc: Python script to work with US populations data
# Purpose: Python script to get the list of files containing information
#          about State, population, year from USdata.io api
#          1. use api to get the data
#          2. get data in a json list
#          3. convert to  table
#          4. clean the data
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


    # Download files
   # Using datausa.io API to form tabular data set of n = 50

    # Connect to datausa.io API get data
    response = requests.get("https://datausa.io/api/data?drilldowns=State&measures=Population&year=latest")

    # save data and meta data in jason
    US_pop_data=  (response.json())

    #the data is clean and does not require any preprocesing
    #
    df_USdata =pd.DataFrame(US_pop_data["data"])

    #remove District of Columbia and puerto Rico from the table
    drop_index = df_USdata[(df_USdata["State"]== 'District of Columbia' )|( df_USdata["State"]== 'Puerto Rico' )].index

    df_USdata.drop(drop_index, inplace = True)
    date = datetime.now().strftime("%Y%m%d")

    # Upload files to AWS S3

    #table["Text"] = table["Text"].apply(remove_non_ascii)

    csv_buffer = StringIO()
    df_USdata.to_csv(csv_buffer,index=False, date_format= '%Y-%m-%d %H:%M:%S')

    awsapi.upload_csv(bucket_name =bucket_name,  key= '{o}'.format(o=object_name)+'{d}'.format(d=date)+'/'+'US_population_data.csv',  body=csv_buffer.getvalue())


        #os.remove(uf)


if __name__ == "__main__":
    main()


