/***********************************************
** File: Assignmend1_ DDLDML.sql
** Desc: creating databases/tables (DDL)
**       uploading  Data from S3 ( DML )
** Auth: Husein Adenwala
** Date: 1/21/2022
************************************************/



-- download extension to connect with aws S3
CREATE EXTENSION aws_s3 CASCADE;




---create new schema and table for USdata(API)

DROP SCHEMA IF EXISTS USdata CASCADE;

CREATE SCHEMA USdata;

DROP TABLE IF EXISTS USdata.US_populationdata

CREATE TABLE USdata.US_populationdata (
   idstate varchar(10) primary key,
   state varchar (30),
   idyear varchar (10),
   year int ,
   population int,
   slugstate varchar(30)
   
);  



--import USdata  data from s3 into US_populationdata table

SELECT aws_s3.table_import_from_s3(
   'USdata.US_populationdata', '', '(format csv , header true)',
   'aws-assignment1-data',
   'USdata/20220121/US_population_data.csv',
   'us-east-2',
   'aws_access_key_id', 'aws_secret_access_key'
);




--create new schema and table for Zillowdata(webscraping)

DROP SCHEMA IF EXISTS Zillowdata CASCADE;
CREATE SCHEMA Zillowdata;

DROP TABLE IF EXISTS Zillowdata.ZillowChicagodata 

CREATE TABLE  Zillowdata.ZillowChicagodata (
   latitude decimal (10,8),
   longitude decimal (11,8),
   floorSize int,
   street varchar (100),
   city varchar (20),
   state varchar (20) ,
   zipcode	varchar (20),
   url varchar (3000),
   price_USD int,
   primary key(latitude, longitude, street ,url)
);

--import Zillow data from s3 into ZillowChicagodata table

SELECT aws_s3.table_import_from_s3(
   'Zillowdata.ZillowChicagodata', '', '(format csv , header true)',
   'aws-assignment1-data',
   'zillowdata/20220121/zillow_chicago_data.csv',
   'us-east-2',
   'aws_access_key_id', 'aws_secret_access_key'
);




 
