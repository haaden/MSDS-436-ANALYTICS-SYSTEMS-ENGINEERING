
#***********************************************
MSDS 436 Final project- Meme stock detection with unsupervised learning
By – Arthur Swanson and Husein Adenwala.
Date - 3/12/2022
#***********************************************/


Project description.
 - End to end process of gathering, preparing data for ML Modeling to Detect Meme Stock using K-means clustering. 



To accrue twitter data:
 - Choose tickers to monitor
 - create S3 bucket
 - Apply for twitter api v2
 - update finalproject_twitter_batch with S3 bucket name, twitter access keys, and ticker list
 - Run finalproject_twitter_batch.py when desired.
	 - To run on AWS, provision EC2 instance with CRON scheduling to run python script
	 - To run locally, create batch script and add job to task scheduler on computer

To add sentiment information:
 - Run MSDS436_finalProject_getS3data.ipynb in jupiter notebook to load data and add sentiment data
 - load data to second s3 bucket/postgres server

To load data into PostgreSQL'
 - To run MSDS436_Final PostgreSQL_DDL.sql

To build model:
 - Run DBSCAN Final MSDS 436.ipynb in google colab. This prevents pyspark issues from local PC setup
 - load output to redshift cluster

To visualize data:
 - load data to redshift: 
	 - make sure public access enabled and TCP port 5439 enabled on security group applied to cluster
	 - use below queries to set up table and load data

create table stuff
(date1 varchar(256),
_open varchar(256),
high varchar(256),
low varchar(256),
_close varchar(256),
adj_close varchar(256),
volume varchar(256),
ticker varchar(256),
date2 varchar(256),
id varchar(256),
date3 varchar(256),
probability varchar(256),
sentiment varchar(256),
clusterid int(4),
PRIMARY KEY ( id )
); -- create redshift table

copy stuff from 's3://msdsredshift436/'
    ACCESS_KEY_ID 'xxxxxxxxxx'
    SECRET_ACCESS_KEY 'xxxxxxxxxxxxxxxxxxxx'
    csv

 - open MSDS436Final.twb and connect to cluster, use custom query: select * from stuff where substring(date2,1,4) = '2022'
 - create visualizations