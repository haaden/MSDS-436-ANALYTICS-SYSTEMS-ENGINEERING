#***********************************************
MSDS 436 Assignment 1
By – Husein Adenwala
Date - 1/23/2022
#***********************************************/

## Assignment description.

Use AWS EC2 services to gather data via API and web scraping in python 3. 
Save the Data in AWS S3 storage
Amazon Aurora PostgreSQL (RDSBM). create schemas and tables and insert date into tables form AWS S3  
Use Amazon Aurora PostgreSQL for generating insights 
Speed up data cleaning and preparation processes by using python klib package to automate data cleaning and type converson (Bonus)




## Scripts: contains the following documents

EC2_comandline.txt has scripts to install packages and scripts to create buckers and run the python scripts mentioned below
awsapi.py python script is used in EC2 to create buckets, folder and subfolder in the S3
USdata.py python script for collecting API data and saving the file in S3 by importing awsapi 
Zillowdata.py python script for webscraping and saving the file in S3 by using awsapi in S3 
Assignmend1_DDLDML.sql  had the DDL and DML scrip to create the two schemas and tables and insert data from S3
Assignment1_Insigts.sql has the 9 SQL queries to provide insisght into the data 
Zillow_klib.py python script for webscraping which automates data cleaning and effeicient type conversion. also saves the file to S3




## Data Sources
.
#API:  Data USA
https://datausa.io/api/data?drilldowns=State&measures=Population&year=latest


# Web scraping: Zillow Chicago housing data
https://www.zillow.com/homes/Chicago,-IL_rb/
